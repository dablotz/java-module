# ── Stage 1: dependency cache ─────────────────────────────────────────────────
# This stage is kept separate so it can be exported and reused in environments
# that have no internet access.
#
# One-time setup (run on a machine with internet access):
#   docker build --target deps -t migrator-deps .
#   docker save migrator-deps | gzip > migrator-deps.tar.gz
#
# Offline build (no internet required after the above):
#   docker load < migrator-deps.tar.gz
#   DOCKER_BUILDKIT=1 docker build --cache-from migrator-deps -t migrator .
FROM maven:3.9-eclipse-temurin-17 AS deps

WORKDIR /build

COPY pom.xml .
RUN mvn --batch-mode dependency:go-offline -q

# ── Stage 2: compile ──────────────────────────────────────────────────────────
FROM deps AS build

COPY src ./src
RUN mvn --batch-mode package -q -DskipTests

# ── Stage 3: runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre

RUN groupadd -r migrator && useradd -r -g migrator migrator

WORKDIR /app

COPY --from=build /build/target/migrator.jar migrator.jar
RUN chown migrator:migrator migrator.jar

USER migrator

EXPOSE 7000

ENTRYPOINT ["java", "-jar", "migrator.jar"]
