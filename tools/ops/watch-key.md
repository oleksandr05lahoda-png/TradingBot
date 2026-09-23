# Ключ сторожа: только чтение

Сторож на ноуте (лаб-бот, `StrategyLab/lab/watch/vps.py`) раз в 15 минут заходит на сервер по SSH.
Сейчас он ходит полным ключом root (`~/.ssh/tradingbot_vps`): ошибка или чужая команда в лаб-боте
могла бы сделать на сервере что угодно.

Отдельный ключ с `command="/opt/watch-status.sh"` умеет ровно одно: напечатать четыре строки
(`status=`, `now=`, `scan=`, `selfcheck=`). Любая команда, которую просит клиент, заменяется этим
скриптом. Нет терминала, нет проброса портов, нет агента. Ключ ничего не пишет и ничего не запускает.

Вывод скрипта байт в байт совпадает с тем, что сторож читает сегодня: проверяет
`tools/ops/test_watch_status.py`.

## ⚠️ Что этот ключ НЕ решает

- Полный ключ `tradingbot_vps` остаётся на том же ноуте. Украденный ноут = доступ к серверу, как и раньше.
  Защита от этого: пароль на полный ключ (шаг 8). Тогда без пароля им не зайти.
- **Не меняйте пока `LAB_VPS_KEY` в `StrategyLab\local.env`.** С 23.09 по этой же переменной ходит
  ночной бэкап лабы (`lab/ops/backup.py`: `find` и `tar` по данным). С ключом сторожа бэкап получит
  вместо архива четыре строки статуса и сломается. Сторожу нужна своя переменная
  (например `LAB_VPS_WATCH_KEY`) в `vps.py`. Это правка в StrategyLab, в этой задаче она не сделана.

Ключ можно поставить уже сейчас: он ничего не ломает и лежит без дела, пока сторож не переключён.

## Строка для authorized_keys

```
command="/opt/watch-status.sh",restrict,no-pty,no-port-forwarding,no-agent-forwarding,no-X11-forwarding,no-user-rc ssh-ed25519 AAAA... lab-watch
```

`restrict` уже включает все запреты. Остальные повторены на случай старого sshd.
Если дома постоянный IP, можно добавить `from="ВАШ.IP"`. Тогда ключ не сработает ни с какого другого адреса.

## Установка (PowerShell 5.1, по одной команде)

Всё выполняется на ноуте. Команда в кавычках после адреса сервера выполняется на сервере.
Внутри неё только одинарные кавычки, удвоенные (`''`): PowerShell 5.1 теряет двойные кавычки внутри
аргументов, и на сервер ушла бы другая команда.

**1. Создать ключ без пароля** (сторож работает сам, вводить пароль некому):

```powershell
ssh-keygen -t ed25519 -f "$env:USERPROFILE\.ssh\tradingbot_watch" -C "lab-watch" -N '""'
```

Меняется: появляются два файла, `tradingbot_watch` и `tradingbot_watch.pub`, в `C:\Users\Asus_F15\.ssh`.

**2. Скопировать скрипт на сервер:**

```powershell
scp -i "$env:USERPROFILE\.ssh\tradingbot_vps" "C:\Users\Asus_F15\IdeaProjects\TradingTelegramBot\tools\ops\watch-status.sh" root@185.183.156.154:/opt/watch-status.sh
```

Меняется: новый файл `/opt/watch-status.sh`.

**3. Убрать виндовые переводы строк, сделать исполняемым и запустить один раз:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'sed -i ''s/\r$//'' /opt/watch-status.sh; chmod 755 /opt/watch-status.sh; /opt/watch-status.sh'
```

Должно напечатать четыре строки: `status=running`, `now=...`, `scan=...`, `selfcheck=...`.

**4. Копия authorized_keys перед правкой:**

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'cp -a /root/.ssh/authorized_keys /root/.ssh/authorized_keys.bak-watch'
```

**5. Собрать строку ключа:**

```powershell
$line = 'command="/opt/watch-status.sh",restrict,no-pty,no-port-forwarding,no-agent-forwarding,no-X11-forwarding,no-user-rc ' + (Get-Content "$env:USERPROFILE\.ssh\tradingbot_watch.pub" -Raw).Trim()
```

**6. Дописать её на сервер** (`tr -d` убирает `\r`, который PowerShell 5.1 добавляет в конец строки):

```powershell
$line | ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'tr -d ''\r'' >> /root/.ssh/authorized_keys'
```

Меняется: в `/root/.ssh/authorized_keys` появляется одна строка в конце. Старые ключи не тронуты.

**7. Проверить оба ключа.**

Новый ключ. Просим `whoami`, а получить должны четыре строки статуса, не `root`:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_watch" -o BatchMode=yes root@185.183.156.154 whoami
```

Полный ключ работает как раньше. Ответ должен быть `root`:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" -o BatchMode=yes root@185.183.156.154 whoami
```

**8. (по желанию, после переключения сторожа) Поставить пароль на полный ключ:**

```powershell
ssh-keygen -p -f "$env:USERPROFILE\.ssh\tradingbot_vps"
```

⚠️ Делать только после того, как сторож и бэкап лабы перестанут ходить полным ключом, иначе оба замолчат.

## Откат

Убрать строку ключа сторожа (остальные ключи не трогаются):

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'sed -i ''/watch-status\.sh/d'' /root/.ssh/authorized_keys'
```

Или вернуть копию из шага 4:

```powershell
ssh -i "$env:USERPROFILE\.ssh\tradingbot_vps" root@185.183.156.154 'cp -a /root/.ssh/authorized_keys.bak-watch /root/.ssh/authorized_keys'
```
