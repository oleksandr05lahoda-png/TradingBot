package com.bot;

import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

public class TelegramBotSender extends TelegramLongPollingBot {

    private final String token = System.getenv("TELEGRAM_TOKEN");
    private final String chatId = System.getenv("CHAT_ID");

    public TelegramBotSender() {
        try {
            // Регистрируем бота в API, чтобы можно было использовать execute()
            TelegramBotsApi botsApi = new TelegramBotsApi(DefaultBotSession.class);
            botsApi.registerBot(this);
        } catch (Exception e) {
            System.out.println("Не удалось зарегистрировать Telegram бота: " + e.getMessage());
        }
    }

    @Override
    public void onUpdateReceived(Update update) {
        // Не используем входящие обновления в этой версии
    }

    @Override
    public String getBotUsername() {
        String name = System.getenv().getOrDefault("BOT_USERNAME", "TradingAIBot");
        return name;
    }

    @Override
    public String getBotToken() {
        return token;
    }

    public void sendSignal(String text) {
        if (token == null || chatId == null) {
            System.out.println("[LOG] sendSignal: " + text);
            return;
        }
        SendMessage msg = SendMessage.builder()
                .chatId(chatId)
                .text(text)
                .build();
        try {
            execute(msg);
        } catch (TelegramApiException e) {
            e.printStackTrace();
        }
    }
}
