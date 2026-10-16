# Reactor runs as a plain Spring Boot fat jar; no container-specific configuration is needed.
# The build stage compiles the whole module tree (tests run separately via .\mvnw.cmd -B verify),
# the runtime stage only carries the JDK and the jar.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN mvn -B -DskipTests -pl app -am package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/app/target/reactor-app-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
