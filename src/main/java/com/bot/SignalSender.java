package com.bot;

import java.io.*;
import java.time.Instant;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

public class SignalSender {

    private final TelegramBotSender telegram;
    private Instant lastSent = Instant.EPOCH;

    public SignalSender() {
        telegram = new TelegramBotSender();
    }

    public void runScheduler() {
        // Запускаем task каждые 5 минут (настраивается ENV INTERVAL_SECONDS, но здесь дефолт 300)
        long intervalSec = Long.parseLong(System.getenv().getOrDefault("INTERVAL_SECONDS", "300"));
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                runOnce();
            }
        }, 0, intervalSec * 1000);
        // держим JVM живой
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void runOnce() {
        try {
            System.out.println("Запуск Python анализа...");
            // В Railway и локально укажи путь к файлу analysis.py как в проекте
            ProcessBuilder pb = new ProcessBuilder("python3", "src/python-core/analysis.py");
            pb.redirectErrorStream(true);
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            boolean anySent = false;
            while ((line = reader.readLine()) != null) {
                System.out.println("Python: " + line);
                // формат ожидаемый: COIN|SIGNAL|CONFIDENCE|CHART_PATH
                try {
                    String[] parts = line.split("\\|");
                    if (parts.length >= 4) {
                        String coin = parts[0].trim();
                        String signal = parts[1].trim();
                        double conf = Double.parseDouble(parts[2].trim());
                        String chart = parts[3].trim();
                        String text = String.format("%s — %s (conf=%.2f)\nChart: %s", coin, signal, conf, chart);
                        telegram.sendSignal(text);
                        anySent = true;
                        lastSent = Instant.now();
                    } else {
                        // если вывод другой — просто отправим как лог (опционально)
                        System.out.println("Непарсируемая строка от Python: " + line);
                    }
                } catch (Exception e) {
                    System.out.println("Ошибка парсинга строки: " + line);
                    e.printStackTrace();
                }
            }

            process.waitFor();

            // если сигналы не отправлялись долго — отправляем уведомление о простое
            int silenceHours = Integer.parseInt(System.getenv().getOrDefault("SILENCE_HOURS", "6"));
            if (!anySent) {
                Duration silence = Duration.between(lastSent, Instant.now());
                if (silence.toHours() >= silenceHours) {
                    String msg = String.format("Внимание: бота не было сигналов %d часов. Продолжаю мониторинг.", silence.toHours());
                    telegram.sendSignal(msg);
                    lastSent = Instant.now();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            // отправим ошибку в Telegram если настроен
            telegram.sendSignal("SignalSender exception: " + e.getMessage());
        }
    }
}
