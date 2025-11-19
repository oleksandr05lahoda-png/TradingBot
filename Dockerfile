FROM openjdk:17
WORKDIR /app

COPY java-bot /app/java-bot
COPY python-core /app/python-core
COPY java-bot/src/main/resources /app/resources

RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install -r python-core/requirements.txt

CMD ["java", "-cp", "java-bot/build/classes/java/main", "com.bot.BotMain"]
