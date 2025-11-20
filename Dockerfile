FROM openjdk:17-slim-buster

RUN apt-get update && \
    apt-get install -y python3 python3-venv python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY gradlew .
COPY gradle/ ./gradle
RUN chmod +x gradlew

COPY build.gradle .
COPY settings.gradle .

COPY src/main/java/ ./java-bot
COPY src/main/resources/ ./resources
COPY src/python-core/ ./python-core

RUN python3 -m venv venv
RUN ./venv/bin/pip install --upgrade pip
RUN ./venv/bin/pip install -r python-core/requirements.txt

RUN ./gradlew build --no-daemon

CMD ["java", "-cp", "java-bot/build/classes/java/main", "com.bot.BotMain"]
