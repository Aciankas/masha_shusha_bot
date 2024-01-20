from aiogram.types import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from aiogram import types
from aiogram.utils.exceptions import MessageToDeleteNotFound, BotBlocked
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.exceptions import InvalidQueryID
from dotenv import load_dotenv, find_dotenv
from dtbase import base, cur, scheduled_be_send, get_slide, delete_scheduled, scheduled_exists, \
    scheduled_coupons_be_closed, cancel_coupon, is_coupon_active, set_st_bots_name, set_start_slide
import os


scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


async def start_apscheduler():
    scheduler.start()


async def on_startup(dp: Dispatcher):
    print('Бот онлайн')
    # asyncio.create_task(scheduler())
    if os.getenv('connectType') == 'webhook':
        await bot.set_webhook(os.getenv('webhookHost'))
    await start_apscheduler()


async def on_shutdown(dp: Dispatcher):
    if os.getenv('connectType') == 'webhook':
        await bot.delete_webhook()
    cur.close()
    base.close()


load_dotenv(find_dotenv())
admin_ids = list(map(int, (os.getenv('adminIDs') or '').split(",")))
moderator_ids = list(map(int, (os.getenv('moderatorIDs') or '').split(",")))
bot_id = (os.getenv('bot_id') or 'info')
storage = MemoryStorage()
bot = Bot(token=os.getenv('Token'))
dp = Dispatcher(bot, storage=storage)
timezone = (int(os.getenv('Timezone') or 3))

set_st_bots_name(bot_id)
set_start_slide(bot_id)


async def group_msg(text, usr_ids=None, content_type=None, content_id=None, keyboard=None, cur_bot=bot, parse_mode=ParseMode.HTML):
    try:
        if usr_ids is None:
            usr_ids = admin_ids
        if type(usr_ids) is int:
            usr_ids = [usr_ids]
        for user in usr_ids:
            if content_type is None:
                await cur_bot.send_message(user, text, reply_markup=keyboard, parse_mode=parse_mode)
            elif content_type == 'photo':
                await cur_bot.send_photo(user,
                                         content_id,
                                         caption=(text or ''),
                                         reply_markup=keyboard,
                                         parse_mode=parse_mode)
            elif content_type == 'video':
                await cur_bot.send_video(user,
                                         content_id,
                                         caption=(text or ''),
                                         reply_markup=keyboard,
                                         parse_mode=parse_mode)
            elif content_type == 'document':
                await cur_bot.send_document(user,
                                            content_id,
                                            caption=(text or ''),
                                            reply_markup=keyboard,
                                            parse_mode=parse_mode)
            elif content_type == 'animation':
                await cur_bot.send_animation(user,
                                             content_id,
                                             caption=(text or ''),
                                             reply_markup=keyboard,
                                             parse_mode=parse_mode)
            elif content_type == 'voice':
                await cur_bot.send_voice(user,
                                         content_id,
                                         caption=(text or ''),
                                         reply_markup=keyboard,
                                         parse_mode=parse_mode)
            elif content_type == 'video_note':
                await cur_bot.send_video_note(user,
                                              content_id,
                                              reply_markup=keyboard)
    except Exception as err:
        await cur_bot.send_message(admin_ids[0], "group_msg error" + str(err) + ": " + str(text) + "|" + str(content_type or 'None') +
                                   "|" + str(content_id or 'None'))


async def delete_message(message: types.Message):
    try:
        await message.delete()
    except MessageToDeleteNotFound:
        await group_msg('Exception: Message.delete()')


@dp.errors_handler(exception=InvalidQueryID)
async def invalid_query_id_handler(update, error):
    await group_msg(f'Ошибка {error}, идентификатор запроса {update} устарел')
    return True


from message_constructor import construct_slide_from_message, slide_button_appears


async def scheduler_job():
    scheduled_messages = scheduled_be_send(bot_id)
    for message in scheduled_messages:
        try:
            slide = get_slide(int(message['slide_id']), bot_id)
            if scheduled_exists(message['send_time'], message['usr_id'], message['slide_id']):
                if await slide_button_appears(slide["appearance_mod"], int(message['usr_id'])):
                    await construct_slide_from_message(slide, int(message['usr_id']), is_bot_msg=True)
                delete_scheduled(message['send_time'], message['usr_id'], message['slide_id'])
        except Exception as err:
            await group_msg(f"ScheduleSlideError: {str(message)}, {err}")
    coupons_canceled = scheduled_coupons_be_closed(bot_id)
    for coupon in coupons_canceled:
        try:
            slide = get_slide(int(coupon['end_slide_id']), bot_id)
            is_active = is_coupon_active(coupon['usr_id'], coupon['coupon_id'])
            cancel_coupon(coupon['end_time'], coupon['usr_id'], coupon['coupon_id'])
            if await slide_button_appears(slide["appearance_mod"], int(coupon['usr_id'])) and is_active:
                await construct_slide_from_message(slide, int(coupon['usr_id']), is_bot_msg=True)
        except Exception as err:
            await group_msg(f"ScheduleCouponError: {str(coupon)}, {err}")


scheduler.add_job(scheduler_job, trigger='interval', seconds=10, id="scheduler_job", replace_existing=True, misfire_grace_time=60*60*12)