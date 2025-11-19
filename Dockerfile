# 1. Базовый образ с Python + JDK 17
FROM python:3.12-slim

# Устанавливаем JDK 17
RUN apt-get update && apt-get install -y openjdk-17-jdk && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копируем Gradle wrapper и скрипты
COPY gradlew .
COPY gradle/ ./gradle
RUN chmod +x gradlew

# Копируем Gradle файлы
COPY build.gradle .
COPY settings.gradle .

# Копируем Java-код
COPY src/main/java/ ./java-bot
COPY src/main/resources/ ./resources

# Копируем Python-код
COPY src/python-core/ ./python-core

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir -r python-core/requirements.txt

# Собираем Java-бота
RUN ./gradlew build --no-daemon

# Запуск: сначала Python анализ, потом Java
CMD ["sh", "-c", "python python-core/analysis.py && java -cp java-bot/build/classes/java/main com.bot.BotMain"]
