# Spring Boot + Kafka Demo (deployable to AWS)

A minimal production-shaped Spring Boot 3.5 (Java 17) service that exposes a REST API, publishes JSON events to Kafka, and consumes them back. Includes Maven build, embedded-Kafka tests, Docker packaging, GitHub Actions CI, and ECS Fargate deployment for AWS.

## What it does

```
POST /api/messages {"content": "hello"}   →  produced to topic `messages`
                                             consumed by KafkaConsumerService (logged)
```

- **Producer** — `KafkaTemplate<String, Message>` with JSON value serialization, `acks=all`, async send with logging callback (`kafka/KafkaProducerService.java`)
- **Consumer** — `@KafkaListener` with 3 worker threads, JSON deserialization to a `record`; failed records are retried twice then routed to `<topic>.DLT` (`kafka/KafkaConsumerService.java`, `config/KafkaConfig.java`)
- **Topic** — auto-created on startup with configurable partitions/replicas via `NewTopic`
- **Ops** — Actuator health/liveness/readiness probes, graceful shutdown, non-root Docker image

All Kafka settings are environment-variable driven, so the same image runs locally and on AWS.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker list |
| `KAFKA_CONSUMER_GROUP` | `kafka-demo-group` | Consumer group id |
| `KAFKA_TOPIC` | `messages` | Topic name |
| `KAFKA_TOPIC_PARTITIONS` / `KAFKA_TOPIC_REPLICAS` | `3` / `1` (3 on MSK) | Topic config |
| `KAFKA_CONSUMER_CONCURRENCY` | `3` | Listener threads |

## Run locally

**Option A — everything in Docker:**

```bash
docker compose up --build
```

**Option B — Kafka in Docker, app in IDE:**

```bash
docker compose up -d kafka
mvn spring-boot:run
```

**Try it:**

```bash
curl -X POST http://localhost:8080/api/messages \
  -H "Content-Type: application/json" \
  -d '{"content": "hello kafka"}'
```

The consumer log line `Received: key=... payload=Message[id=..., content=hello kafka]` confirms the round trip.

## Build & test

```bash
mvn verify        # tests run against an embedded Kafka broker
mvn package       # target/*.jar
docker build -t kafka-demo .   # multi-stage build, JRE runtime image
```

## GitHub Actions

- `.github/workflows/ci.yml` — builds and tests on every push/PR to `main`, uploads the JAR artifact.
- `.github/workflows/deploy.yml` — on push to `main`: build → test → build image → push to ECR → render ECS task definition → deploy to an ECS Fargate service and wait for stability.

## Deploying to AWS (ECS Fargate + Amazon MSK)

One-time setup — see the AWS docs for [ECS getting started](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_GetStarted_EC2.html) and [MSK getting started](https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html):

1. **Infrastructure** (via console, CLI, or Terraform/CloudFormation):
   - VPC with public + private subnets
   - Amazon MSK cluster (3 brokers) in private subnets; grab the bootstrap brokers string
   - ECR repository, e.g. `springboot-kafka-demo`
   - ECS cluster (Fargate), service with an ALB targeting port 8080, task execution role + task role, CloudWatch log group `/ecs/springboot-kafka-demo`
2. **Task definition** — `aws/task-definition.json` is a starting point; fill the `REPLACE_WITH_*` values (brokers, role ARNs, ECR image URI, region) and register it:
   ```bash
   aws ecs register-task-definition --cli-input-json file://aws/task-definition.json
   ```
   Then create/update the ECS service to use family `springboot-kafka-demo`.
3. **GitHub repo configuration** (Settings → Secrets and variables → Actions):
   - Secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
   - Variables: `ECR_REPOSITORY`, `ECS_CLUSTER`, `ECS_SERVICE`, `TASK_FAMILY`, `CONTAINER_NAME`
   - Recommended: an IAM user/role scoped to ECR push + ECS deploy only.
4. Push to `main` — CI tests, the image lands in ECR, and ECS rolls out a new revision. Watch the service with:
   ```bash
   aws ecs describe-services --cluster <cluster> --services <service>
   ```

**Note on MSK networking:** the ECS tasks must run in the same VPC (or a peered one) with a security group allowing traffic to the MSK brokers on port 9092 (or 9098 if TLS/IAM auth is enabled). If using MSK IAM auth, add the `aws-msk-iam-auth` dependency and SASL config — that piece is intentionally left out of this demo.

## Project layout

See **PROJECT.md** for the full implementation walkthrough — every design decision, config value, and trade-off explained.

```
src/main/java/com/example/kafkademo/
├── KafkaDemoApplication.java        # entry point
├── config/KafkaConfig.java          # topic bean + consumer error handler
├── controller/MessageController.java# REST endpoint
├── kafka/KafkaProducerService.java  # producer
├── kafka/KafkaConsumerService.java  # consumer
└── model/Message.java               # JSON event payload
```
