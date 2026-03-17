# ARGUS Ingestion API - Simplified Single-Stage Dockerfile
# This version uses the JAR built on the host for reliability and speed.
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# Create a non-root user for security
RUN groupadd -r spring && useradd -r -g spring spring

# Copy the GeoIP database (make sure GeoLite2-City.mmdb is in the same folder as Dockerfile)
RUN mkdir -p /app/geoip
COPY GeoLite2-City.mmdb /app/geoip/GeoLite2-City.mmdb
RUN chown -R spring:spring /app/geoip

# Copy the JAR from the local target/ directory
COPY target/ingestion-api-0.0.1-SNAPSHOT.jar app.jar
RUN chown spring:spring app.jar

USER spring:spring

EXPOSE 8100

# Optimized container memory settings
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]