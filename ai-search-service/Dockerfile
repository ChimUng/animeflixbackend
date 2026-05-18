# Stage 1: Build
FROM maven:3.9.9-eclipse-temurin-21 AS builder
WORKDIR /app

# Copy pom.xml trước để cache dependency layer (tăng tốc build)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build jar (bỏ qua test để build nhanh)
RUN mvn clean package -DskipTests

# Stage 2: Run (Runtime)
FROM eclipse-temurin:21-jdk
WORKDIR /app

# Copy file JAR từ stage build
COPY --from=builder /app/target/ai-search-service-0.0.1-SNAPSHOT.jar app.jar

# Expose port (mặc định Spring Boot là 8080, bạn có thể đổi nếu cần)
EXPOSE 8080

# Chạy với profile local (giống các service khác)
ENTRYPOINT ["java", "-jar", "app.jar", "--spring.profiles.active=local"]