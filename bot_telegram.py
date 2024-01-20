from aiogram.utils import executor
from create_bot import dp, bot, admin_ids, on_startup, on_shutdown, bot_id, timezone
from handlers import client, admin, other
import datetime
import os


def exec_group_msg(text, usr_ids=admin_ids, cur_bot=bot, cur_dispatcher=dp):
    for user in usr_ids:
        executor.start(cur_dispatcher, cur_bot.send_message(user, text))


admin.register_handlers_admin()
client.register_handlers_client()
other.register_handlers_other()

exec_group_msg(str(datetime.datetime.now()) + ': Бот запущен\nМетод: ' + (os.getenv('connectType') or 'null') +
               '\nhttps://api.telegram.org/bot' + (os.getenv('Token') or '') +
               '/setWebhook?url=' + (os.getenv('webhookHost') or '') +
               f'\nbot_id = {bot_id}' +
               f'\ntimezone = {timezone}' +
               f'\nadmin_ids = {admin_ids}' +
               '\n/start')


print('Запуск бота...')
if os.getenv('connectType').strip() == 'webhook':
    print('Бот запущен с помощью webhook')
    executor.start_webhook(
        dispatcher=dp,
        webhook_path='',
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=(os.getenv('skip_updates') == 'True' or True),
        host=(os.getenv('webhookIP') or '0.0.0.0'),
        port=int(os.environ.get("PORT", (os.getenv('webhookPort') or '5000')))
    )
elif os.getenv('connectType').strip() == 'polling':
    print('Бот запущен с помощью polling')
    executor.start_polling(dp, skip_updates=(os.getenv('skip_updates') == 'True' or True), on_startup=on_startup)
else:
    print(f"Некорректный os.getenv('connectType') = {os.getenv('connectType') or 'null'}")

print('Завершение работы...')
exec_group_msg('Бот выключен')
