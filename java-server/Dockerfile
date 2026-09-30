# syntax=docker/dockerfile:1.7
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
COPY server/pom.xml server/
COPY jmh/pom.xml jmh/
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -pl server -am dependency:go-offline
COPY server/src server/src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -pl server -am -DskipTests package

FROM eclipse-temurin:21-jre
RUN useradd --create-home --uid 10001 fanout
COPY --from=build /src/server/target/fanout-server.jar /app/fanout-server.jar
USER fanout
EXPOSE 7000
# Container-aware heap sizing; G1 is the JDK 21 default collector.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+UseG1GC", "-jar", "/app/fanout-server.jar"]
