# =========================
# Dockerfile для TradingTelegramBot
# =========================

# Базовый образ с Java 17 (Eclipse Temurin)
FROM eclipse-temurin:17-jdk

# Рабочая директория внутри контейнера
WORKDIR /app

# Копируем Gradle wrapper и скрипты
COPY gradlew ./gradlew
COPY gradle/ ./gradle
COPY build.gradle ./build.gradle
COPY settings.gradle ./settings.gradle

# Копируем Java-код бота
COPY java-bot/ ./java-bot

# Копируем Python-часть и shared конфиги
COPY python-core/ ./python-core
COPY shared/ ./shared

# Делаем Gradle сборку Java бота
RUN ./gradlew build

# Установка Python и pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Установка зависимостей Python
RUN pip3 install --no-cache-dir -r python-core/requirements.txt

# Команда запуска бота
CMD ["java", "-cp", "java-bot/build/classes/java/main", "com.bot.BotMain"]
