FROM eclipse-temurin:21-jdk-jammy AS builder

WORKDIR /workspace
COPY gradlew settings.gradle build.gradle gradle.properties /workspace/
COPY gradle /workspace/gradle
COPY modules /workspace/modules

RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :modules:exchange-app:bootJar -x test

FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /workspace/modules/exchange-app/build/libs/*-boot.jar /app/exchange.jar

# Run as root so the mounted /data volume is writable. For a production
# deployment we would prepare /data ownership via init container or volume
# host-side chown; this image is a dev/test reference.
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -Xms1g -Xmx2g -XX:+AlwaysPreTouch \
  --enable-native-access=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

EXPOSE 8080 9001 9100 9200

ENTRYPOINT ["sh", "-c", "mkdir -p /data && exec java $JAVA_OPTS -jar /app/exchange.jar"]
