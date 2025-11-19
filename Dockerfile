# Используем образ с Python и OpenJDK 17
FROM eclipse-temurin:17-jdk

# Устанавливаем Python
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копируем Gradle wrapper
COPY gradlew .
COPY gradle/ ./gradle
RUN chmod +x gradlew

# Копируем файлы сборки Gradle
COPY build.gradle .
COPY settings.gradle .

# Копируем Java-код
COPY src/main/java/ ./java-bot
COPY src/main/resources/ ./resources

# Копируем Python-код
COPY src/python-core/ ./python-core
COPY src/python-core/requirements.txt ./python-core/

# Устанавливаем Python-зависимости
RUN pip3 install --no-cache-dir -r python-core/requirements.txt

# Собираем Java-бота
RUN ./gradlew build --no-daemon

# Запуск бота
CMD ["java", "-cp", "java-bot/build/classes/java/main", "com.bot.BotMain"]
