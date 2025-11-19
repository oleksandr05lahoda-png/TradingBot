FROM openjdk:17-jdk

# Рабочая директория в контейнере
WORKDIR /app

# Копируем Gradle скрипты и wrapper
COPY gradlew ./gradlew
COPY gradle/ ./gradle
COPY build.gradle ./build.gradle
COPY settings.gradle ./settings.gradle

# Копируем Java код и ресурсы
COPY src/main/java ./java-bot
COPY src/main/resources ./resources

# Копируем Python код
COPY src/python-core ./python-core

# Делаем сборку Java бота через Gradle
RUN ./gradlew build --no-daemon

# Запуск бота
CMD ["java", "-cp", "java-bot/build/classes/java/main", "com.bot.BotMain"]
