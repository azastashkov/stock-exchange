FROM eclipse-temurin:21-jdk-jammy AS builder

WORKDIR /workspace
COPY gradlew settings.gradle build.gradle gradle.properties /workspace/
COPY gradle /workspace/gradle
COPY modules /workspace/modules

RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :modules:load-client:installDist -x test

FROM eclipse-temurin:21-jre-jammy

RUN groupadd --system exchange && useradd --system --gid exchange --no-create-home exchange
WORKDIR /app

COPY --from=builder /workspace/modules/load-client/build/install/load-client /app/

USER exchange
ENV PATH="/app/bin:${PATH}"

ENTRYPOINT ["/app/bin/load-client"]
