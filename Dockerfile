# Базовый образ JDK 17
FROM openjdk:17-jdk-slim
WORKDIR /app

# Копируем файлы проекта
COPY java-bot/ ./java-bot
COPY python-core/ ./python-core
COPY shared/ ./shared
COPY build.gradle ./build.gradle
COPY gradlew ./gradlew
COPY gradle/ ./gradle
COPY settings.gradle ./settings.gradle

# Сборка Java-бота
RUN ./gradlew build

# Установка Python
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install --no-cache-dir -r python-core/requirements.txt

# Запуск бота
CMD ["java", "-cp", "java-bot/build/classes/java/main", "com.bot.BotMain"]
