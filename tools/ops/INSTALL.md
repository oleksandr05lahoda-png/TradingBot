# Установка на сервер: бот, ops-скрипты, cron

Для владельца. Всё запускается **с ноутбука** в PowerShell 5.1, по одной команде в блоке.
Команда в кавычках после `root@185.183.156.154` выполняется **на сервере**.
Внутри неё только одинарные кавычки, удвоенные (`''`): PowerShell 5.1 теряет двойные кавычки
внутри аргументов, и на сервер ушла бы другая команда.

Сервер ничего не делает сам: каждый шаг запускаете вы.

## Что меняется

| Что | Где на сервере | Что меняется |
|---|---|---|
| Бот (Java) и сканер | контейнер `tradingbot` | новый образ, прежний остаётся как `tradingbot:prev` |
| `vps-deploy.sh` | `/opt/tradingbot/tools/` | сначала тесты, потом сборка; копия данных; ждёт готовности; `rollback` |
| `digest.py` | `/opt/digest.py` | 24.09: больше не запускается; в репо изменён только комментарий, на сервер не ставится |
| `selfcheck.py` | `/opt/selfcheck.py` | 24.09: молчит про вставший контейнер, если watchdog уже ДОСТАВИЛ сообщение об этой остановке |
| `watchdog.sh` | `/opt/watchdog.sh` | 24.09: флаг «сказал» ставится только после доставки; не доставил → повтор через 5 мин |
| `pautina.py`, `crowd.py`, `balance_probe.py` | `/opt/` | **ничего**: в репо лежат копии, байт в байт как на сервере |
| crontab | root | 24.09: без строки `digest.py`, раздел «24.09: одна панель» |
| копии данных | `/opt/backups/data-*.tgz` | появляются при каждой выкатке, хранятся последние 7 |

Сверено 23.09 (md5 на сервере = md5 в репо):

```
7fe5f6a501c5a98b7441a2952bc21bb4  digest.py        (старый; в репо уже новый)
047ef86b6866b3f197538b9dc21ffeda  pautina.py
abefe1d58841958876b9fc794bff38ec  crowd.py
c5b485cd4d9446c9cab458193c6d05d3  watchdog.sh
b3c4a52eaef623cb51468d3a98263c10  balance_probe.py
ca6abc71caa6a883daa9445f70020696  crontab.txt
```

24.09 в репо (на сервере станут такими после раздела «24.09: одна панель»):

```
14fbfd8987fdb4b82efe3a4d058ba862  selfcheck.py
c5f91dcc6dc7acec37c19460bcfad55c  watchdog.sh
ddddf4ee2a6301051b3e45f1eeb8aee3  crontab.txt      (без строки digest.py)
bdc9382165f1b826a30d5be2b81018d3  digest.py        (только комментарий; на сервере остаётся 48fb56b8...)
```

## 1. Проверить на ноуте

```powershell
cd C:\Users\Asus_F15\IdeaProjects\TradingTelegramBot
```

```powershell
Get-ChildItem tools\scanner\test_*.py, tools\ops\test_*.py | ForEach-Object { $r = py -3 -B $_.FullName | Select-Object -Last 1; "{0,-26} {1}" -f $_.Name, $r }
```

У каждого файла последняя строка `ALL CHECKS PASSED`, `all checks passed`, `ALL OK` или `OK`.
Если есть `FAILED`, дальше не идём.

Меняется: ничего.

## 2. Копия нынешнего дерева на сервере

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'cp -a /opt/tradingbot /opt/tradingbot.bak-$(date +%Y%m%d-%H%M); ls -d /opt/tradingbot.bak-*'
```

Меняется: появляется папка `/opt/tradingbot.bak-ДАТА`. Бот не тронут.

## 3. Скопировать файлы на сервер

```powershell
scp -r -i "$env:USERPROFILE\.ssh\tradingbot_vps" gradle gradlew build.gradle settings.gradle src tools Dockerfile root@185.183.156.154:/opt/tradingbot/
```

Меняется: файлы в `/opt/tradingbot` перезаписаны новыми. Работающий бот не тронут:
он работает из образа, а не из этой папки.

## 4. Убрать виндовые переводы строк из скриптов

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'sed -i ''s/\r$//'' /opt/tradingbot/gradlew /opt/tradingbot/tools/*.sh /opt/tradingbot/tools/ops/*.sh'
```

Меняется: только концы строк в `.sh` и `gradlew`. Если файлы уже чистые, ничего.

## 5. Выкатка

```powershell
ssh -t -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'bash /opt/tradingbot/tools/vps-deploy.sh'
```

По порядку, скрипт:
1. Спрашивает Binance, не забанен ли адрес.
2. Проверяет, что на диске есть 2 ГБ.
3. Прогоняет офлайн-тесты сканера и ops. **Хоть один красный → стоп. Ничего не собрано, бот работает как работал.**
4. Собирает образ. Ошибка сборки тоже не трогает бота.
5. Копирует `/opt/tradingbot-data` в `/opt/backups/data-ДАТА.tgz`.
6. Запоминает образ, который торгует сейчас, как `tradingbot:prev`. Только если контейнер работает и ни разу не перезапускался; иначе прежний `:prev` остаётся.
7. Останавливает бота штатно, до 45 с на завершение. Стопы остаются на бирже. Обрыв SSH с этого места и до запуска скрипт не прерывает.
8. Запускает новый образ и ждёт до 6 минут, пока бот прочитает счёт, а сканер стартует. Потом ещё минуту смотрит, что контейнер не падает.

В конце итог на один экран:

```
== итог: выкатка
   сборка       20260923-180000Z no-git
   autoscan.py  <md5> = исходник
   бот          готов
   сканер       запущен
   позиций      15 (по логу бота)
   перезапусков 0
   откат        bash /opt/tradingbot/tools/vps-deploy.sh rollback
```

Если бот `НЕ ответил` или сканер `НЕ запустился`, смотрим лог: `docker logs tradingbot`.
Если скрипт написал `контейнер упал`, новый образ не держится: нужен откат (шаг 8).
Бан Binance при загрузке бот пересиживает сам, перезапускать не надо.

Меняется: образ и контейнер бота; одна новая копия в `/opt/backups`.

## 6. Ops-скрипты и crontab

**6.1 Копия нынешних скриптов и crontab:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'd=/opt/ops-backup-$(date +%Y%m%d-%H%M); mkdir -p $d; cp -a /opt/digest.py /opt/selfcheck.py /opt/watchdog.sh /opt/pautina.py /opt/crowd.py /opt/balance_probe.py $d/; crontab -l > $d/crontab.txt; ls $d'
```

Меняется: новая папка `/opt/ops-backup-ДАТА` с копиями. Больше ничего.

**6.2 Сравнить, что лежит на сервере и что пришло из репо:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'cd /opt/tradingbot/tools/ops; for f in digest.py selfcheck.py watchdog.sh pautina.py crowd.py balance_probe.py; do if cmp -s $f /opt/$f; then echo same $f; else echo DIFF $f; fi; done; if crontab -l | cmp -s - crontab.txt; then echo same crontab; else echo DIFF crontab; fi'
```

Ожидается: `DIFF digest.py`, `DIFF selfcheck.py`, у остальных `same`.
С 24.09 ещё `DIFF watchdog.sh` и `DIFF crontab`: это раздел «24.09: одна панель», ниже.
Если `DIFF` у чего-то ещё, значит на сервере правили руками. **Стоп**, сначала разобраться.

Меняется: ничего.

**6.3 Пробный запуск новой сводки** (в Telegram придёт сводка и график, как вечером):

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'python3 /opt/tradingbot/tools/ops/digest.py'
```

В конце строка `digest sent`. Если пришёл только текст с `🟠 График не собрался`, текст всё равно верный.

Меняется: одно сообщение в Telegram. На сервере ничего.

**6.4 Поставить новые скрипты:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'install -m 755 /opt/tradingbot/tools/ops/digest.py /opt/digest.py; install -m 755 /opt/tradingbot/tools/ops/selfcheck.py /opt/selfcheck.py; md5sum /opt/digest.py /opt/selfcheck.py'
```

Меняется: `/opt/digest.py` и `/opt/selfcheck.py`. Cron запускает их по-старому, строки cron не трогаются.

**6.5 crontab: только если в 6.2 было `DIFF crontab`:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'crontab /opt/tradingbot/tools/ops/crontab.txt; crontab -l'
```

Меняется: crontab root становится таким, как в репо. С 24.09 в репо он без сводки: раздел ниже.

## 24.09: одна панель

Ваше решение 24.09: вечерней сводки в 18:00 больше нет, всё смотрится в `/panel` лаба-бота.
И одна авария = одно сообщение: если контейнер встал, пишет watchdog, самопроверка молчит
(в логе и для внешнего пульса поломка остаётся). Но молчит, только если сообщение watchdog
**дошло** и относится **к этой** остановке. Не дошло, флаг остался от старой аварии, или watchdog
в ту же секунду записал ошибку доставки: самопроверка пишет сама. Лучше два сообщения, чем ни одного.
Меняются три вещи: crontab (без строки `digest.py`), `/opt/selfcheck.py` и `/opt/watchdog.sh`.
`digest.py` остаётся в репо и на сервере, просто не запускается.

Сначала шаги 3 и 4 (файлы из репо на сервере; шаг 4 убирает `\r` и из `watchdog.sh`). В 6.2 теперь
будет `DIFF digest.py` (только комментарий, на сервер не ставится, так и останется),
`DIFF selfcheck.py`, `DIFF watchdog.sh` и `DIFF crontab`. Так и должно быть. `DIFF` у чего-то
другого: **Стоп**.

**Копия нынешнего:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'd=/opt/ops-backup-$(date +%Y%m%d-%H%M); mkdir -p $d; cp -a /opt/selfcheck.py /opt/watchdog.sh $d/; crontab -l > $d/crontab.txt; ls $d'
```

**Новые самопроверка и watchdog** (порядок не важен: новая самопроверка со старым watchdog тоже
не промолчит, она видит его ошибку доставки):

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'install -m 755 /opt/tradingbot/tools/ops/selfcheck.py /opt/selfcheck.py; install -m 755 /opt/tradingbot/tools/ops/watchdog.sh /opt/watchdog.sh; md5sum /opt/selfcheck.py /opt/watchdog.sh; bash -n /opt/watchdog.sh && echo watchdog syntax ok'
```

Ожидается `14fbfd8987fdb4b82efe3a4d058ba862` у `selfcheck.py`, `c5f91dcc6dc7acec37c19460bcfad55c`
у `watchdog.sh` и строка `watchdog syntax ok`. Другой md5 у `watchdog.sh` обычно значит `\r` в конце
строк: шаг 4 не сделан.

**Новый crontab:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'sed -i ''s/\r$//'' /opt/tradingbot/tools/ops/crontab.txt; crontab /opt/tradingbot/tools/ops/crontab.txt; crontab -l; crontab -l | md5sum'
```

Ожидаются 4 строки: `watchdog.sh`, `selfcheck.py`, `pautina.py`, `crowd.py`. Строки с `digest.py` нет.
md5 `ddddf4ee2a6301051b3e45f1eeb8aee3`.

**Проверка:** одна строка `checks: ...`, в Telegram ничего, если всё в порядке:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'python3 /opt/selfcheck.py'
```

Меняется: crontab root, `/opt/selfcheck.py` и `/opt/watchdog.sh`. Бот не тронут.

**Вернуть сводку в 18:00** (если захотите):

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 '(crontab -l; echo ''0 18 * * * /usr/bin/python3 /opt/digest.py >> /opt/tradingbot-data/ops/digest.log 2>&1'') | crontab -; crontab -l'
```

**Откат всего раздела:** вместо `ДАТА` имя папки из копии выше (`ls -d /opt/ops-backup-*`):

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'cp -a /opt/ops-backup-ДАТА/selfcheck.py /opt/ops-backup-ДАТА/watchdog.sh /opt/; crontab /opt/ops-backup-ДАТА/crontab.txt; crontab -l'
```

## 7. Проверка

Бот жив, самопроверка идёт, копия данных на месте:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'docker inspect -f {{.State.Status}} tradingbot; tail -2 /opt/tradingbot-data/ops/selfcheck.log; ls -lh /opt/backups'
```

Самопроверка вручную (одна строка `checks: 0 fail(s)`, в Telegram ничего, если всё в порядке):

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'python3 /opt/selfcheck.py'
```

Ещё `/status` в боте в Telegram.

Меняется: ничего.

## 8. Откат

**Бот:** вернуть образ, который торговал до выкатки (те же флаги запуска, копия данных перед этим):

```powershell
ssh -t -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'bash /opt/tradingbot/tools/vps-deploy.sh rollback'
```

Меняется: контейнер снова на прежнем образе. Неудачный образ остаётся как `tradingbot:bad`.
Если и откат `не поднялся`, второй `rollback` не поможет: он запустит тот же образ. Смотрим `docker logs tradingbot`.

**Ops-скрипты:** сначала узнать имя папки с копией:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'ls -d /opt/ops-backup-*'
```

Потом вернуть оба файла. Вместо `ДАТА` подставьте из ответа:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'cp -a /opt/ops-backup-ДАТА/digest.py /opt/ops-backup-ДАТА/selfcheck.py /opt/'
```

**Дерево исходников:** вернуть папку из шага 2. Новая не удаляется, а откладывается:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'mv /opt/tradingbot /opt/tradingbot.bad-$(date +%Y%m%d-%H%M); cp -a /opt/tradingbot.bak-ДАТА /opt/tradingbot'
```

**Данные** (`/opt/backups/data-*.tgz`) возвращать только при порче журнала или леджера.
Бот при этом должен стоять. Старый леджер против живых позиций даёт «неизвестные» позиции.
Это не делается одной командой: сначала разбор.

## 9. Ключ сторожа

Отдельно: `tools/ops/watch-key.md`. Ключ только для чтения для сторожа на ноуте.
Ставится независимо от шагов выше.

## 10. CI

`.github/workflows/ci.yml` на каждый push и PR гоняет Java-тесты (JDK 21) и офлайн-тесты
Python (3.12). Заработает после `git push` на GitHub. Пушите вы: отсюда ничего не отправлялось.
