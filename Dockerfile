# Используем стабильный JDK 17 образ
FROM eclipse-temurin:17-jdk

# Рабочая директория
WORKDIR /app

# Копируем Gradle wrapper и скрипты
COPY gradlew .
COPY gradle/ ./gradle

# Делаем gradlew исполняемым
RUN chmod +x gradlew

# Копируем build.gradle и settings
COPY build.gradle .
COPY settings.gradle .

# Копируем Java-код
COPY src/main/java/ ./java-bot
COPY src/main/resources/ ./resources

# Копируем Python-код
COPY src/python-core/ ./python-core

# Сборка Java бота
RUN ./gradlew build --no-daemon

# Запуск бота
CMD ["java", "-cp", "java-bot/build/classes/java/main", "com.bot.BotMain"]
