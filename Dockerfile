# -------------------------------
# Dockerfile для TradingTelegramBot
# -------------------------------

# Базовый образ JDK 17
FROM eclipse-temurin:17-jdk
WORKDIR /app

# Копируем Gradle и скрипты
COPY gradlew ./gradlew
COPY gradle/ ./gradle
COPY build.gradle ./build.gradle
COPY settings.gradle ./settings.gradle

# Копируем Java-код и ресурсы (относительно Dockerfile)
COPY src/main/java/ ./java-bot
COPY src/main/resources/ ./resources

# Копируем Python-код
COPY src/main/python-core/ ./python-core

# Сборка Java-бота через Gradle
RUN ./gradlew build

# Установка Python и pip
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install --no-cache-dir -r python-core/requirements.txt

# Запуск бота
CMD ["java", "-cp", "java-bot/com/bot", "BotMain"]
