# Используем образ с Python и JDK
FROM eclipse-temurin:17-jdk

# Устанавливаем Python
RUN apt-get update && apt-get install -y python3 python3-venv python3-pip && rm -rf /var/lib/apt/lists/*

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

# Создаём виртуальное окружение для Python
RUN python3 -m venv venv
RUN ./venv/bin/pip install --upgrade pip
RUN ./venv/bin/pip install -r python-core/requirements.txt

# Сборка Java бота
RUN ./gradlew build --no-daemon

# Запуск бота, используем Python из venv
CMD ["sh", "-c", "./venv/bin/python src/python-core/analysis.py && java -cp java-bot/build/classes/java/main com.bot.BotMain"]
