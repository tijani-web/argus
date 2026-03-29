# ARGUS Ingestion API - Multi-Stage Build
# Stage 1: Build the JAR using Maven
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn clean package -DskipTests -q

# Stage 2: Runtime image
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Create a non-root user for security
RUN groupadd -r spring && useradd -r -g spring spring

# Create GeoIP directory and download database
RUN mkdir -p /app/geoip && \
    curl -L -f "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb" -o /app/geoip/GeoLite2-City.mmdb || \
    (echo "Falling back to different GeoIP source..." && \
     curl -L -f "https://raw.githubusercontent.com/GitSquared/node-geolite2-redist/master/redist/GeoLite2-City.mmdb" -o /app/geoip/GeoLite2-City.mmdb) && \
    chown -R spring:spring /app/geoip

# Copy the JAR from the build stage
COPY --from=build /build/target/ingestion-api-0.0.1-SNAPSHOT.jar app.jar
RUN chown spring:spring app.jar

USER spring:spring

EXPOSE 8100

# Optimized container memory settings
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]