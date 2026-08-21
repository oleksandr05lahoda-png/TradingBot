# The machine is two processes: the Java risk core and the Python scanner that feeds
# it. Railpack builds one language, so the image is hand-written. They share a book
# file and the scanner reads the bot's log to see a halt, exactly as on the laptop —
# splitting them into two services would break both channels.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY gradle gradle
COPY gradlew build.gradle settings.gradle ./
RUN chmod +x gradlew && ./gradlew --no-daemon dependencies --quiet || true
COPY src src
RUN ./gradlew --no-daemon shadowJar -x test --quiet

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/build/libs/bot.jar /app/bot.jar
COPY tools/scanner/autoscan.py /app/scanner/autoscan.py
COPY tools/docker-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Everything that must survive a restart lives here; Railway mounts a volume on it.
# Without persistence the stop-id ledger is lost, reconciliation reports every open
# position as unknown, and the bot boots halted and never recovers.
ENV DATA_DIR=/app/data
VOLUME /app/data

ENTRYPOINT ["/app/entrypoint.sh"]
