FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src/ ./src/
RUN mvn package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/flink-trade-job-1.0-SNAPSHOT.jar .
CMD ["java", "--add-opens", "java.base/java.util=ALL-UNNAMED", "--add-opens", "java.base/java.time=ALL-UNNAMED", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "-jar", "flink-trade-job-1.0-SNAPSHOT.jar"]
