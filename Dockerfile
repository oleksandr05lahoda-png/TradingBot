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
# A bounded build JVM: this image is built on the 2 GB VPS beside the live bot, and an unbounded
# Gradle daemon is the one thing on that host that could push the trading process into swap.
RUN ./gradlew --no-daemon -Dorg.gradle.jvmargs=-Xmx640m shadowJar -x test --quiet

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
# What code is trading: the deploy script passes the build time, the commit and a -dirty mark,
# and the entrypoint logs it on every boot. Without it nothing could prove which tree was live.
ARG BUILD_STAMP=unknown
RUN echo "$BUILD_STAMP" > /app/BUILD_STAMP
COPY --from=build /src/build/libs/bot.jar /app/bot.jar
COPY tools/scanner/autoscan.py /app/scanner/autoscan.py
COPY tools/docker-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Everything that must survive a restart lives here. The mount itself is declared in
# Railway (a VOLUME instruction is rejected by its builder), so this is only the path
# both processes agree on. Without persistence the stop-id ledger is lost,
# reconciliation reports every open position as unknown, and the bot boots halted.
ENV DATA_DIR=/app/data

ENTRYPOINT ["/app/entrypoint.sh"]
