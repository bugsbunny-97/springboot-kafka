# ---- build stage ----
FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /workspace

# Cache dependencies first
COPY pom.xml .
RUN mvn -B -ntp dependency:go-offline

COPY src src
RUN mvn -B -ntp package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:17-jre-jammy
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1001 app

WORKDIR /app
COPY --from=build /workspace/target/*.jar app.jar

USER app
EXPOSE 8080
ENV JAVA_OPTS=""

# Container-level healthcheck (ECS/EC2); Kubernetes users should rely on probes instead.
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
