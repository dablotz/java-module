# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build

# Cache dependency resolution separately from source compilation.
# Only re-downloads deps when pom.xml changes.
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -q -DskipTests

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre

RUN groupadd -r migrator && useradd -r -g migrator migrator

WORKDIR /app

COPY --from=build /build/target/migrator.jar migrator.jar
RUN chown migrator:migrator migrator.jar

USER migrator

# /data is the conventional mount point for input CSVs and output JSON.
# Example:
#   docker run --rm -v $(pwd)/data:/data migrator \
#     --input /data/region_a.csv /data/region_b.csv \
#     --output /data/result.json
VOLUME ["/data"]

ENTRYPOINT ["java", "-jar", "migrator.jar"]
