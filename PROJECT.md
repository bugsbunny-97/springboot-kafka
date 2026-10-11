# PROJECT.md — Implementation Details & Design Decisions

Deep-dive companion to the README. Every file, every config value, and every trade-off
explained. Use it to understand, review, defend, or extend this project.

---

## 1. The system at a glance

```
                 HTTP POST /api/messages {"content": "..."}
                        │
                        ▼
              ┌───────────────────┐
              │  MessageController │  Spring MVC, generates UUID if absent
              └─────────┬─────────┘
                        │ Message (Java record, JSON)
                        ▼
              ┌───────────────────┐   KafkaTemplate (JSON serializer, acks=all)
              │ KafkaProducerService├───────────────┐
              └───────────────────┘               │
                                                    ▼
                                    Kafka topic "messages" (3 partitions,
                                    created at startup by KafkaAdmin)
                                                    │
                                                    ▼
              ┌───────────────────┐  @KafkaListener (3 threads, JSON deserializer)
              │ KafkaConsumerService│  on failure: 2 retries → topic "messages.DLT"
              └───────────────────┘

  Packaging:   multi-stage Docker (JDK17 build → JRE17 runtime, non-root)
  Local:       docker-compose = KRaft Kafka (no ZooKeeper) + app container
  CI/CD:       GitHub Actions: test → build image → push ECR → deploy ECS Fargate
  AWS target:  ECS Fargate tasks in private subnets → Amazon MSK brokers
```

One service is deliberately both producer **and** consumer of the same topic so a
single deployable demonstrates the full round trip end to end.

---

## 2. Technology choices and why

| Choice | Alternatives considered | Why this one |
|---|---|---|
| Spring Boot 3.5.x | Quarkus, Micronaut, plain Spring | Largest ecosystem, best Kafka integration (`spring-kafka`), teams hiring for it. Boot 3.5 is the current stable line. |
| Java 17 | 21 (LTS) | 17 is the widest-supported LTS for Boot 3 and the base images; nothing here needs 21 features. Bumping is a one-line change (`<java.version>`). |
| `spring-kafka` | Plain `kafka-clients`, Alpakka | Removes 90% of boilerplate: auto-configured `KafkaTemplate`, `@KafkaListener`, container factory, error handlers, admin. |
| Apache Kafka 3.9 (KRaft) | ZooKeeper-based cluster, Redpanda, MSK Serverless | KRaft is the current default (ZooKeeper removed in 4.0); same client protocol as MSK. |
| JSON via Spring's `JsonSerializer` | Avro/Protobuf + Schema Registry | Zero infra for a demo; type headers disabled so the contract is explicit (see §3.1). Schema Registry is the right upgrade for multi-team environments. |
| Maven | Gradle | Requested. Boot parent POM pins tested plugin/dependency versions. |
| ECS Fargate | EKS, EC2, Lambda (MSK lanes for Lambda exist) | Simplest "serverless containers" path with GitHub's first-party actions; no cluster autoscaling/K8s ops burden for one service. |
| Amazon MSK | Self-managed Kafka on EC2, Confluent Cloud | Managed brokers in your VPC, same Kafka wire protocol, IAM auth integration. |
| GitHub Actions | Jenkins, GitLab, CodePipeline | Native to GitHub; AWS publishes maintained official actions for ECR/ECS. |

---

## 3. Application code walkthrough

### 3.1 Event model — `model/Message.java`

```java
public record Message(String id, String content) { }
```

Decisions:

- **Java `record`** — immutable, value-based `equals`/`hashCode`, Jackson deserializes
  records natively since 2.12. No mutable event state = no accidental mutation
  between produce and consume.
- **`id` doubles as the Kafka message key** (`KafkaProducerService.send`). Kafka
  guarantees ordering *per partition*, and records with the same key always land on
  the same partition. So all events with the same id stay ordered — that's the only
  ordering guarantee Kafka gives you, and this keeps it.
- **JSON without type headers.** The producer sets `spring.json.add.type.headers: false`
  and the consumer sets `spring.json.use.type.headers: false` +
  `spring.json.value.default.type: com.example.kafkademo.model.Message`.
  Default behavior would embed the producer's fully-qualified class name in a
  `__TypeId__` header and have the consumer instantiate *that* class. Problems:
  (a) renaming/refactoring the producer class silently breaks every consumer;
  (b) the consumer trusts a producer-controlled header to pick a class — mild
  security smell. Disabling both makes the JSON itself the contract and the type
  explicit on the consumer side. This is the trade you accept when skipping a
  schema registry.

### 3.2 REST endpoint — `controller/MessageController.java`

- `POST /api/messages` returns **202 Accepted**, not 200/201. The write is
  *accepted for publication*, not completed — Kafka send is async, so the honest
  semantic is 202. The response body carries the generated `id` and `status: queued`.
- If the request omits `id`, the controller generates a UUID. Key assignment happens
  once, at the edge, so retries from clients (or future idempotency logic) reuse the
  same key.
- Validation is intentionally absent (no `@Valid`, no `@NotBlank`) — demo scope;
  there's no invalid state a two-field record can reach that would harm Kafka.
  First thing to add for real use, along with an `@ControllerAdvice` error shape.

### 3.3 Producer — `kafka/KafkaProducerService.java`

```java
kafkaTemplate.send(topic, message.id(), message)
    .whenComplete((result, ex) -> { ... log partition/offset or error ... });
```

- **Async fire-and-forget with observability.** We do *not* block the HTTP thread on
  the broker round trip (`.get()`/`.join()` would add broker latency + backpressure
  into request latency). The `whenComplete` callback logs the committed
  partition/offset on success — those two numbers are your evidence a message
  actually landed, and are what you'll correlate in support situations.
- The callback runs on the producer's I/O thread — it must stay cheap; it only logs.
- Producer config: `acks=all` (blocks until all in-sync replicas acknowledge — the
  durability setting; `acks=1` can lose data on follower failover). Since
  kafka-clients 3.x, `enable.idempotence` defaults to **true**, so retries cannot
  create duplicate entries per partition.
- `max.block.ms: 10000` (default 60s). This bounds how long `send()` may block when
  metadata is unavailable (broker down at boot, topic not yet creatable). Without
  this, a downed Kafka means every HTTP request hangs up to a minute before erroring;
  with it, requests fail in ≤10s. Discovered this live during sandbox verification —
  the POST returned a 500 after exactly 60s with no broker.

### 3.4 Consumer — `kafka/KafkaConsumerService.java`

```java
@KafkaListener(topics = "${app.topic.name}")
public void onMessage(ConsumerRecord<String, Message> record) { ... }
```

- Listener takes `ConsumerRecord`, not the bare payload, so partition/offset/key are
  logged alongside the value — the minimum viable observability for "did my consumer
  process what the producer sent".
- `spring.kafka.listener.concurrency: 3` spins 3 consumer threads in the container.
  Two rules that make "3" meaningful:
  1. Concurrency > partition count = idle threads (a partition is consumed by at most
     one consumer in a group). Topic has 3 partitions → 3 threads exactly saturate it.
  2. More app instances than partitions = idle instances. This is the horizontal
     scaling ceiling for consumers: **scale by adding partitions first**.
- `auto-offset-reset: earliest` — a brand-new consumer group starts at the beginning
     of retained history rather than skipping to "now". Right for demos and for
     replay-oriented event pipelines; use `latest` where consumers only care about
     post-deployment traffic.
- Offset commits: spring-kafka overrides the raw client's `enable.auto.commit=true`
  with its container-managed acknowledgments (default `AckMode.BATCH` — commit after
  each polled batch is processed). Net effect: at-least-once delivery; a crash
  mid-batch replays the uncommitted records, so consumer logic must be idempotent.

### 3.5 Topic provisioning + failure handling — `config/KafkaConfig.java`

**Topic bean:**
```java
@Bean
public NewTopic messagesTopic(...) { TopicBuilder.name(name)
        .partitions(partitions).replicas(replicas).build(); }
```
Boot auto-registers a `KafkaAdmin` and applies any `NewTopic` beans at startup —
infrastructure-as-code for the topic, no manual `kafka-topics.sh`. Partition and
replica counts are env-driven (`KAFKA_TOPIC_PARTITIONS`, `KAFKA_TOPIC_REPLICAS`)
because they differ by environment: **1 replica** for local single-broker, **3**
for MSK. Note: partition count can be raised later (new consumers rebalance onto
them) but never lowered; and key→partition mapping changes on resize, so ordered
keys can interleave during a resize window.

**Error handler:**
```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    return new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(template),
            new FixedBackOff(1000L, 2));
}
```
- Boot's `KafkaAnnotationDrivenConfiguration` injects any `CommonErrorHandler` bean
  into the listener container factory automatically — no customizer needed. (The
  older `ConcurrentKafkaListenerContainerFactoryCustomizer` hook no longer exists in
  Boot 3.5; verified by decompiling the autoconfigure jar when compilation failed.)
- Semantics: a record that throws is retried **2 times, 1s apart, in-memory**; if it
  still fails it is published to `<topic>.DLT` (the recoverer's default naming,
  same partition) and the consumer moves on. This prevents **poison-pill stalls** —
  one malformed record looping forever and blocking the whole partition.
- `FixedBackOff` (fixed interval) over `ExponentialBackOff`: for a 2-retry budget
  the complexity of exponential backoff buys nothing.
- Why DLQ over skip-and-log: failed records stay *retrievable* — reprocess
  `messages.DLT` with a fixed consumer after deploying the bug fix, instead of
  digging through logs. A DLQ consumer/alerting on DLT depth is the natural next
  addition (see §10).

### 3.6 `application.yml` decisions

Every environment-varying knob is `${ENV:default}`, so **one image runs everywhere**
(12-factor; the same principle that makes the ECS task definition trivial):

| Key | Why |
|---|---|
| `bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}` | Broker list changes per env: compose service name, MSK bootstrap string. |
| `admin.fail-fast: false` + 5s/10s admin timeouts | If Kafka is unreachable at boot (rolling deploy, broker maintenance), KafkaAdmin logs and the app still starts — the health-check window isn't burned waiting on it. Also cuts the no-broker startup stall from 43s to ~13s (measured). Topic creation then relies on broker auto-create or a later restart. |
| `server.shutdown: graceful` | On SIGTERM (ECS stop), in-flight HTTP requests complete before Tomcat stops; combined with spring-kafka's container shutdown, the in-flight poll batch finishes and its offsets commit before exit → clean deploys without a redelivery burst. |
| `management.endpoint.health.probes.enabled: true` | Exposes `/actuator/health/liveness` and `/actuator/health/readiness` — the standard signals Kubernetes-style orchestrators want (ECS doesn't require them, but ALB/target and any future K8s move do). |
| `logging.level.org.apache.kafka: warn` | The client is chatty at INFO (connection retries every second when a broker is down, as seen in verification logs); WARN keeps operational noise out of CloudWatch. |

---

## 4. Failure behavior map (what actually happens when…)

| Failure | Behavior | Why acceptable / what to improve |
|---|---|---|
| Broker down when POST arrives | Send blocks up to 10s (`max.block.ms`), then logs + HTTP 500 | Requests fail fast-ish; add a 503 + Retry-After via `@ControllerAdvice` for polish |
| Broker down at app boot | KafkaAdmin logs, app still serves (13s vs 43s before the admin timeout fix) | Consumer retries broker connection indefinitely; no crash loop |
| Consumer throws on a record | 2 in-memory retries → published to `messages.DLT` → partition keeps flowing | DLQ is reprocessable; add DLT-depth alerting |
| App killed mid-batch | Uncommitted offsets → redelivery of those records after restart | At-least-once ⇒ consumer must be idempotent; fine for this demo |
| Bad JSON on topic (not a `Message`) | Deserialization error → same retry→DLT path | Poison pills can't stall the pipeline |
| ECS task fails health check | ECS stops routing; with deployment circuit breaker enabled, rolls back to previous revision | See §9.4 |

---

## 5. Testing strategy — `src/test/.../KafkaRoundTripTest.java`

```java
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "${app.topic.name}")
```

- `@EmbeddedKafka` boots a **real in-memory broker** (not a mock) on a random port
  and transparently re-points `spring.kafka.bootstrap-servers` at it. Full wire
  protocol: serialization, broker round trip, topic auto-create — all exercised.
- The test publishes through the *real* `KafkaProducerService`, then consumes with a
  consumer built from the auto-configured `consumerFactory` under a separate group.
  Assertion is on **what's actually in the topic** (key, payload) rather than on the
  app listener's side effect — the listener only logs, and asserting on log output
  couples the test to logging config.
- `KafkaTestUtils.getSingleRecord` polls until a record arrives — no flaky sleeps.
- Two tests, both passing: the round trip and a sanity check the broker is actually
  embedded.

What's deliberately *not* covered (and the honest alternatives):
- **HTTP layer** — `@WebMvcTest` for the controller (400 on malformed JSON, 202
  shape) is cheap to add; skipped to keep one meaningful test rather than three trivial ones.
- **DLT path** — would need a listener that throws on a sentinel payload plus an
  await on the `messages.DLT` record; worthwhile once business logic exists to fail.
- **Testcontainers** — `@Container` Kafka would test against the *identical* image
  used in compose/production, but requires a Docker daemon in CI. Embedded broker
  runs anywhere; that portability won for this repo.

Beyond unit tests, this project was verified live in a sandbox: `mvn verify` green,
then a real Kafka 3.9 KRaft broker + the built jar, `POST /api/messages` → producer
log (partition/offset) → consumer log (`Received: …`) → `kafka-console-consumer`
showed the JSON payload. The `max.block.ms` and admin-timeout settings above came
directly out of that session.

---

## 6. Docker packaging — `Dockerfile`

Multi-stage, 31 lines, decisions in order of appearance:

1. **Build stage = JDK 17, runtime = JRE 17.** Nothing in the running app needs
   `javac`; the JRE image is ~40% smaller and has a smaller patch surface.
2. **`COPY pom.xml` + `dependency:go-offline` before `COPY src`** — dependency layer
   is cached until the pom changes; source-only edits rebuild in seconds. This is
   the single biggest CI-speed lever in a Maven Dockerfile.
3. **`apt-get install curl` in the runtime image** — only for the `HEALTHCHECK`
   (the JRE image ships no curl). `--no-install-recommends` + apt cache purge keeps
   bloat minimal.
4. **Non-root `app` user (uid 1001)** — container-escape blast-radius reduction;
   also required by most org security policies and Fargate runs fine as non-root.
5. **`HEALTHCHECK` via curl → `/actuator/health`** — this is *Docker's* check. ECS
   ignores it (it uses the task-def `healthCheck`), but it makes `docker run` and
   compose behave correctly standalone. Three layers of health exist here on
   purpose: Docker (local), ECS (deployment gating), ALB (traffic routing).
6. **`ENTRYPOINT sh -c "java $JAVA_OPTS …"`** — `JAVA_OPTS` is the seam for
   environment-specific JVM tuning (e.g. `-XX:MaxRAMPercentage=75.0` on Fargate
   where the default 25% of 1 GiB heap is conservative).
7. **`.dockerignore`** — excludes `.git`, `target/` (fresh build inside the image),
   `aws/`, `.github/`. Smaller context = faster builds, and no local build
   artifacts leaking into images.

---

## 7. Local stack — `docker-compose.yml`

- **`apache/kafka:3.9.0` in KRaft mode** (`process.roles: broker,controller`,
  quorum voter on :9093) — no ZooKeeper container, matching where Kafka itself is
  headed (removed in 4.0).
- **Dual listeners — the part that bites everyone.** Kafka brokers *tell clients
  which host:port to use for subsequent connections* (`advertised.listeners`). One
  address cannot serve both container clients and host clients:
  - `PLAINTEXT://kafka:29092` — what containers see (Docker DNS name `kafka`)
  - `PLAINTEXT_HOST://localhost:9092` — what your laptop sees (host tools, the
    app run from IDE, `kafka-console-consumer` on the host)
  The first cut of this file advertised `localhost:9092` only, which works for
  host clients but makes the **app container** try to reach `localhost:9092`
  *inside itself* — the classic compose+Kafka failure. Fixed to the standard
  dual-listener pattern; the app container bootstraps to `kafka:29092`.
- Single-broker overrides (`offsets/transaction replication factor = 1`,
  `auto-create-topics: true`) — MSK equivalents are 3 and usually false.
- `depends_on: kafka` is start-order only (no health gate); the producer's
  retry/backoff tolerates the broker still warming up.

---

## 8. CI/CD — `.github/workflows/`

### `ci.yml` — quality gate (push/PR to main)
- Temurin 17 + `cache: maven` (hashes pom; deps download once per dependency change).
- `mvn verify` = compile + embedded-Kafka tests + package.
- Uploads `target/*.jar` as a 7-day artifact — a known-good jar without any AWS
  coupling; deploys don't need it, but it makes the workflow independently useful.

### `deploy.yml` — push-to-deploy (main)
1. **Config guard step** — checks all six required secrets/variables exist and
   fails with a per-name error instead of an opaque AWS error mid-pipeline.
2. `mvn verify` again — deploy path never trusts an untested commit.
3. `configure-aws-credentials@v4` (scoped keypair) → `amazon-ecr-login@v2`.
4. Image built once, tagged **both** `:latest` and `:<git sha>`; both pushed.
   Tagging by sha = immutable, auditable, rollback-able (`git log` ↔ ECR ↔ running
   task); `:latest` is convenience for humans.
5. `describe-task-definition` → `jq` strips AWS-injected read-only fields
   (ARN, revision, status…) so the JSON can be re-registered as a new revision.
6. `amazon-ecs-render-task-definition@v1` swaps only the image URI (all other task
   config — env vars, roles, log groups — stays authoritative in AWS, not in git).
7. `amazon-ecs-deploy-task-definition@v2` updates the service and
   `wait-for-service-stability: true` blocks until the new revision is stable —
   the workflow run itself becomes the deploy audit record.

Why the "download + re-render" pattern instead of committing the task def: it means
in-console tweaks to memory/env aren't silently reverted by the next push. The
`aws/task-definition.json` in-repo is the *initial* seed (step 2 of README setup),
not the live source of truth. The trade-off is drift — pick per org norms.

---

## 9. AWS production topology

### 9.1 Architecture
```
ALB (public subnets) ──▶ ECS Fargate service (≥2 tasks, private subnets, awsvpc ENIs)
                                │  SG: allows 9092/9098 → MSK SG
                                ▼
                       Amazon MSK (3 brokers, multi-AZ, private subnets)
ECS tasks also: egress via NAT gw, logs → CloudWatch "/ecs/springboot-kafka-demo"
```
- **Fargate** = no instance management; each task gets its own ENI (`awsvpc`).
- **MSK in the same VPC** — Kafka is a stateful, connection-heavy protocol; you
  don't want it crossing the internet. Same-VPC = private DNS resolution of
  bootstrap brokers + security-group-to-security-group rules.
- **≥2 tasks** for availability; consumer group rebalancing handles the rest
  (3 partitions ⇒ at most 3 tasks do consuming work; extra tasks still serve HTTP).

### 9.2 Task definition (`aws/task-definition.json`)
- `512 CPU / 1024 MiB` — smallest comfortable JVM slice; scale memory before CPU.
  Default JVM ergonomics take 25% of container RAM as max heap (~256 MiB here) —
  raise via `JAVA_OPTS=-XX:MaxRAMPercentage=75.0` if GC pressure shows up.
- Env vars mirror application.yml keys — `KAFKA_TOPIC_REPLICAS: 3` for MSK.
- `executionRoleArn` (pull image + logs) is separate from `taskRoleArn` (app's AWS
  permissions) — least privilege by construction; today the task role needs *nothing*.
- `healthCheck` (curl → actuator, `startPeriod: 60` for JVM boot + broker connect).
- `awslogs` driver → CloudWatch; `awslogs-stream-prefix: ecs` gives
  `prefix/container/task-id` stream naming — task-ID in the stream is what makes
  "logs for this failing task" a one-liner.

### 9.3 Networking requirements (the usual silent killers)
- Task SG → MSK SG on **9092** (plaintext/TLS) or **9098** (IAM/TLS) must be allowed.
- MSK has no public endpoint: tasks and brokers must share a VPC (or peering/TGW).
- The ALB health check must hit `/actuator/health` (200) — default `/` works too
  since it 404s-healthy by default mapping… configure the target group path explicitly.

### 9.4 Safe rollouts & rollback
- Deploy waits for service stability; enable the **deployment circuit breaker** so a
  failed rollout auto-rolls-back instead of parking:
  `aws ecs update-service --cluster <c> --service <s> --deployment-configuration deploymentCircuitBreaker={enable=true,update=true}`
- Graceful shutdown means the old task drains in-flight requests; consumers commit
  offsets on stop, so the replacement task resumes without replay storms.

### 9.5 Known gap — MSK IAM auth (be honest about it)
MSK clusters commonly enforce **IAM/TLS auth** (port 9098). This demo speaks
plaintext. To close the gap: add
`software.amazon.msk:aws-msk-iam-auth` to the pom, set
`security.protocol: SASL_SSL`, `sasl.mechanism: AWS_MSK_IAM`,
`sasl.jaas.config: software.amazon.msk.auth.iam.IAMLoginModule required;`,
`sasl.client.callback.handler.class: software.amazon.msk.auth.iam.IAMClientCallbackHandler`,
and grant the *task role* `kafka-cluster:Connect` on the cluster. ~20 lines total;
intentionally left out so the demo runs with zero AWS-specific code locally.

---

## 10. Deliberate scope cuts (and the order to add them)

1. **DLT consumer + alerting** — the DLQ exists but nothing watches it; next step is
   a listener on `messages.DLT` + a CloudWatch metric/alarm on its offset lag.
2. **Input validation + error responses** — `@Valid` on the DTO, `@ControllerAdvice`
   mapping send failures to 503.
3. **MSK IAM auth** — §9.5, mandatory before real AWS use with default MSK settings.
4. **Observability** — Micrometer + CloudWatch EMF (or Prometheus) for
   producer/consumer lag; actuator alone doesn't expose Kafka lag.
5. **Schema Registry (Avro/Protobuf)** — when a second consumer team appears.
6. **Terraform for infra** — the console/CLI setup in the README is the manual path;
   codifying VPC/MSK/ECR/ECS makes the whole thing reproducible.
7. **Persistence** — consumers currently log; a sink (RDS/DynamoDB) is where
   business value actually accrues.

---

## 11. File map

```
pom.xml                                     Boot 3.5.5 parent, Java 17, web+actuator+kafka
src/main/java/com/example/kafkademo/
├── KafkaDemoApplication.java               @SpringBootApplication entry point
├── config/KafkaConfig.java                 NewTopic bean (partitions/replicas env-driven),
│                                           DefaultErrorHandler → 2 retries → DLT (§3.5)
├── controller/MessageController.java       POST /api/messages → 202, UUID key gen (§3.2)
├── kafka/KafkaProducerService.java         async send + whenComplete logging (§3.3)
├── kafka/KafkaConsumerService.java         @KafkaListener, 3 threads, record+metadata log (§3.4)
└── model/Message.java                      record(id, content) — the JSON contract (§3.1)
src/main/resources/application.yml          all knobs env-var driven (§3.6)
src/test/java/.../KafkaRoundTripTest.java   embedded-broker produce→consume (§5)
Dockerfile                                  multi-stage JDK→JRE, non-root, healthcheck (§6)
docker-compose.yml                          KRaft Kafka + app, dual listeners (§7)
.github/workflows/ci.yml                    build+test gate, JAR artifact (§8)
.github/workflows/deploy.yml                ECR push → task-def render → ECS deploy (§8)
aws/task-definition.json                    Fargate seed task def (§9.2)
README.md                                   quick start + AWS setup runbook
PROJECT.md                                  this document
```
