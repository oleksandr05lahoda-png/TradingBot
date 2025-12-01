package com.bot;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;

public class BotMain {

    public static void main(String[] args) {
        String tgToken = "ВАШ_TELEGRAM_BOT_TOKEN";
        String chatId = "ВАШ_CHAT_ID";

        TelegramBotSender telegram = new TelegramBotSender(tgToken, chatId);
        SignalSender signalSender = new SignalSender(telegram);

        signalSender.start();

        System.out.println("[" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] Бот запущен и работает!");
    }
}
