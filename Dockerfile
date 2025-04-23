FROM maven:3.8-openjdk-17 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src
COPY resources ./resources

# Build the application
RUN mvn clean package -DskipTests

FROM openjdk:17-slim

WORKDIR /app
COPY --from=builder /app/target/*.jar ./app.jar
COPY --from=builder /app/resources ./resources

# Environment variables for configuration
ENV FLINK_TEST_RESOURCES_PATH=/app/resources/execute_tests

# Default command to run tests
CMD ["java", "-jar", "app.jar"] 