import datetime
import re

from aiogram import types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode
from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaAudio, InputMediaAnimation, InputMediaDocument
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, UserDeactivated, MessageNotModified, WrongFileIdentifier
from psycopg2.extras import RealDictRow

from dtbase import get_mediagroup, get_keyboard, click_log, user_active_slides, create_scheduled, delete_for_blocked, \
    create_scheduled_coupon, get_coupon_by_id, coupons_for_course, is_coupon_active, spec_to_text, get_medialist_first, \
    is_course_paid, get_slide
from create_bot import group_msg, bot, bot_id, timezone, admin_ids


def nvl(val, non_val):
    if val is None:
        return non_val
    else:
        return val


def get_weekday(weekday, week=None, hour=None, minute=None):
    if week is None:
        week = 0
    if hour is None:
        hour = 12
    if minute is None:
        minute = 0
    return datetime.datetime.combine((datetime.datetime.now() + datetime.timedelta(days=week*7 + (weekday - datetime.datetime.now().weekday() - 1)%7 + 1)), datetime.time(hour, minute)) - datetime.timedelta(hours=timezone)


def get_day(day, hour=None, minute=None):
    if hour is None:
        hour = 12
    if minute is None:
        minute = 0
    return datetime.datetime.combine((datetime.datetime.now() + datetime.timedelta(days=day)), datetime.time(hour, minute)) - datetime.timedelta(hours=timezone)


def get_minutes_from_now(minutes):
    return datetime.datetime.now() + datetime.timedelta(minutes=minutes)


def convert_spec_message_text(text, user_id=None):  # спец текст находится между %spc%
    try:
        if text is None or text.find('%spc%') == -1:
            return text
        else:
            cur_pos = 0
            while text.find('%spc%', cur_pos) != -1:
                cur_pos = text.find('%spc%', cur_pos)
                open_pos = cur_pos + 5
                close_pos = text.find('%/spc%', open_pos)
                if cur_pos != -1:
                    text = text.replace(text[open_pos:close_pos], str(spec_to_text(text[open_pos:close_pos], user_id)), 1).replace('%spc%', '', 1).replace('%/spc%', '', 1)
                else:
                    break
            return text.replace('%spc%', '').replace('%/spc%', '')
    except Exception as err:
        print('ERR convert_spec_message_text: ' + str(err))
        return str(text) + '\n\nconversion_error: ' + str(err)


async def schedule_create(string, user_id):  # 'weekday=0,week=0,hour=13,minute=30,slide=2;day=2,hour=10,minute=20,slide=3;minutes=30,slide=4'
    schedules_list = string.split(';')
    for element in schedules_list:
        try:
            schedule = dict(re.findall(r'(\w+)=(\w+)', element))
            schedule_parsed = None
            if 'weekday' in schedule:
                schedule_parsed = get_weekday(weekday=int(schedule['weekday']),
                                              week=int(schedule.get('week') or 0),
                                              hour=int(schedule.get('hour') or 12),
                                              minute=int(schedule.get('minute') or 0))
            elif 'day' in schedule:
                schedule_parsed = get_day(day=int(schedule['day']),
                                          hour=int(schedule.get('hour') or 12),
                                          minute=int(schedule.get('minute') or 0))
            elif 'minutes' in schedule:
                schedule_parsed = get_minutes_from_now(int(schedule['minutes']))
            if schedule_parsed is None:
                raise ValueError
            else:
                create_scheduled(schedule_parsed, user_id, int(schedule['slide']))
        except Exception as err:
            await group_msg(f"Schedule parsing error at '{element}': {err}")


async def coupon_schedule_create(user_id, coupon_id):  # 'weekday=0,week=0,hour=13','minute=30;day=2,hour=10,minute=20','minutes=30','20.06.2024 12:00'
    try:
        coupon = get_coupon_by_id(coupon_id)
        schedule_parsed = None
        if re.match(r'\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}', coupon["schedule_set"]):
            schedule_parsed = datetime.datetime.strptime(coupon["schedule_set"], '%d.%m.%Y %H:%M') - datetime.timedelta(hours=timezone)
        else:
            schedule = dict(re.findall(r'(\w+)=(\w+)', coupon["schedule_set"]))
            if 'weekday' in schedule:
                schedule_parsed = get_weekday(weekday=int(schedule['weekday']),
                                              week=int(schedule.get('week') or 0),
                                              hour=int(schedule.get('hour') or 12),
                                              minute=int(schedule.get('minute') or 0))
            elif 'day' in schedule:
                schedule_parsed = get_day(day=int(schedule['day']),
                                          hour=int(schedule.get('hour') or 12),
                                          minute=int(schedule.get('minute') or 0))
            elif 'minutes' in schedule:
                schedule_parsed = get_minutes_from_now(int(schedule['minutes']))
        if schedule_parsed is None:
            raise ValueError
        else:
            create_scheduled_coupon(schedule_parsed, user_id, coupon_id, coupon["charges"])
    except Exception as err:
        await group_msg(f"Coupon schedule parsing error at user = {user_id}, coupon = {coupon_id}': {err}")


class SlideError(Exception):
    pass


async def slide_button_appears(appearance_mod, user_id=None):
    try:
        if appearance_mod is None:
            return True
        try:
            appearance = appearance_mod.split('=')
            mod = appearance[0]
            elements = appearance[1].split(",")
        except Exception as err:
            await group_msg(f'slide_button_appears validation error: {err}')
            return True
        if user_id is None or appearance is None:
            return True
        elif mod == 'coupon_expire':
            return is_coupon_active(user_id, elements[0])
        elif mod == 'course_paid':
            return is_course_paid(user_id, elements[0])
        elif mod == 'course_not_paid':
            return not is_course_paid(user_id, elements[0])
        else:
            visited_count = user_active_slides(elements, user_id)
            if mod == 'disappear_any':
                return visited_count == 0
            elif mod == 'disappear_all':
                return visited_count < len(elements)
            elif mod == 'appear_any':
                return visited_count > 0
            elif mod == 'appear_all':
                return visited_count == len(elements)
    except Exception as err:
        await group_msg(f'slide_button_appears error: {err}')


async def construct_keyboard_from_select(select, user_id=None):
    if select is not None:
        try:
            cur_row_list = []
            cur_row = 1
            keyboard_list = []
            for button in select:
                if await slide_button_appears(button["appearance_mod"], user_id):
                    if cur_row_list != [] and cur_row != button["row_num"]:
                        keyboard_list.append(cur_row_list)
                        cur_row = button["row_num"]
                        cur_row_list = []
                    if button["modifier"] is not None:
                        cur_row_list.append(InlineKeyboardButton(text=button["name"], callback_data=button["modifier"]))
                    elif button["url"] is not None:
                        cur_row_list.append(InlineKeyboardButton(text=button["name"], url=button["url"]))
                    else:
                        cur_row_list.append(
                            InlineKeyboardButton(text=button["name"], callback_data=f'slide_{button["slide_link"]}'))
            keyboard_list.append(cur_row_list)
            keyboard = InlineKeyboardMarkup()
            for button in keyboard_list:
                keyboard.row(*button)
            return keyboard
        except Exception as err:
            raise err
    else:
        return None


async def construct_keyboard(slide_id, user_id=None, modifier=None, applied_answers=None):  # ✅
    select = get_keyboard(slide_id)
    if modifier in ('quest_buttons', 'quest_multiple'):
        for button in select:
            button['modifier'] = f"answer={button['row_num']},{button['row_pos']};slide={slide_id}"
        if modifier == 'quest_multiple':
            select.append(RealDictRow({'row_num': 999, 'row_pos': 999, 'slide_id': slide_id, 'slide_link': None, 'name': 'Отправить ответы', 'url': None, 'modifier': 'quest_commit', 'appearance_mod': None}))
            if applied_answers:
                for answer in applied_answers:
                    for dict_row in select:
                        if int(dict_row["row_num"]) == int(answer["row_num"]) and int(dict_row["row_pos"]) == int(answer["row_pos"]):
                            dict_row["name"] = f"✅{dict_row['name']}"
                            break
    try:
        return await construct_keyboard_from_select(select, user_id)
    except Exception as err:
        await group_msg(f'keyboard error: slide={slide_id}, err={err}')
        return None


async def construct_slide(slide, msg, is_bot_msg=False):
    try:
        user = msg.from_user
        # if type(msg) is types.CallbackQuery:
        #     msg = msg.message
        if await slide_button_appears(slide["appearance_mod"], user.id):
            return await construct_slide_from_message(slide, user.id, is_bot_msg)
        return False
    except SlideError:
        raise
    except Exception as err:
        await group_msg(f"SlideError: {err}")
        raise SlideError(err)


async def construct_slide_from_message(slide, user_id, is_bot_msg=False):
    try:
        msg = None
        spec_text = ''
        if user_id in admin_ids:
            spec_text = f'\n\n/slide_{slide["id"]}'
        keyboard = InlineKeyboardMarkup()
        if slide["type"] == "medialist":
            first_media = get_medialist_first(slide["media_id"])
            slide["type"] = first_media["type"]
            mediagroup_id = slide["media_id"]
            slide["media_id"] = first_media["media_id"]
            slide["modifier"] = "medialist"
        click_log(user_id, slide['id'], is_bot_msg)
        if slide["schedule_set"] is not None:
            await schedule_create(slide["schedule_set"], user_id)
        modifier = nvl(slide["modifier"], 'null')
        if modifier == 'empty':
            pass
        elif 'redirect' in modifier:
            try:
                mod_value = str(slide['modifier']).split('=')[1]
                msg = await construct_slide_from_message(get_slide(mod_value, bot_id), user_id, is_bot_msg)
            except Exception as err:
                await group_msg(f'slide_redirect_error: slide={slide["id"]}; err - {err}')
        else:
            if modifier in ('quest_buttons', 'quest_multiple'):
                keyboard = await construct_keyboard(slide['id'], user_id, modifier=modifier)
            else:
                keyboard = await construct_keyboard(slide['id'], user_id)

            if 'coupon_start' in modifier:
                await coupon_schedule_create(user_id, int(slide["modifier"].split('_')[2]))
            elif 'course_price' in modifier:
                coupons = coupons_for_course(user_id, int(slide["modifier"].split('_')[2]))
                for coupon in coupons:
                    keyboard.row(InlineKeyboardButton(text=coupon["name"], callback_data=f'coupon_use={slide["modifier"].split("_")[2]},{coupon["id"]}'))

            slide['message'] = convert_spec_message_text(slide['message'], user_id)

            if slide['type'] is None:
                msg = await bot.send_message(user_id,
                                             (slide['message'] or '') + spec_text,
                                             protect_content=True,
                                             reply_markup=keyboard,
                                             parse_mode=ParseMode.HTML)
            elif slide['type'] == 'photo':
                msg = await bot.send_photo(user_id,
                                           slide['media_id'],
                                           caption=(slide['message'] or '') + spec_text,
                                           protect_content=True,
                                           reply_markup=keyboard,
                                           parse_mode=ParseMode.HTML)
            elif slide['type'] == 'video':
                msg = await bot.send_video(user_id,
                                           slide['media_id'],
                                           caption=(slide['message'] or '') + spec_text,
                                           protect_content=True,
                                           reply_markup=keyboard,
                                           parse_mode=ParseMode.HTML)
            elif slide['type'] == 'document':
                msg = await bot.send_document(user_id,
                                              slide['media_id'],
                                              caption=(slide['message'] or '') + spec_text,
                                              protect_content=True,
                                              reply_markup=keyboard,
                                              parse_mode=ParseMode.HTML)
            elif slide['type'] == 'gif':
                msg = await bot.send_animation(user_id,
                                               slide['media_id'],
                                               caption=(slide['message'] or '') + spec_text,
                                               protect_content=True,
                                               reply_markup=keyboard,
                                               parse_mode=ParseMode.HTML)
            elif slide['type'] == 'voice':
                msg = await bot.send_voice(user_id,
                                           slide['media_id'],
                                           caption=(slide['message'] or '') + spec_text,
                                           protect_content=True,
                                           reply_markup=keyboard,
                                           parse_mode=ParseMode.HTML)
            elif slide['type'] == 'video_note':
                msg = await bot.send_video_note(user_id,
                                                slide['media_id'],
                                                protect_content=True,
                                                reply_markup=keyboard,
                                                parse_mode=ParseMode.HTML)
            elif slide['type'] == 'mediagroup':
                mediagroup_select = get_mediagroup(slide["media_id"])
                mediagroup_list = []
                for key, media_id in mediagroup_select.items():
                    if media_id is not None:
                        m_id = media_id.split(' ')[0]
                        m_type = media_id.split(' ')[1]
                        if m_type == 'photo':
                            mediagroup_list.append(InputMediaPhoto(m_id))
                        elif m_type == 'video':
                            mediagroup_list.append(InputMediaVideo(m_id))
                        elif m_type == 'voice':
                            mediagroup_list.append(InputMediaAudio(m_id))
                        elif m_type == 'gif':
                            mediagroup_list.append(InputMediaAnimation(m_id))
                        elif m_type == 'document':
                            mediagroup_list.append(InputMediaDocument(m_id))
                msg = await bot.send_media_group(user_id,
                                                 mediagroup_list,
                                                 protect_content=True,
                                                 parse_mode=ParseMode.HTML)
            else:
                await group_msg(f'slide_type_error: slide={slide["id"]}, ')
                raise SlideError

            if 'medialist' in modifier:
                keyboard = InlineKeyboardMarkup().row(
                    InlineKeyboardButton(text='◀️', callback_data=f'medialist_back={msg.chat.id},{msg.message_id},{mediagroup_id},1'),
                    InlineKeyboardButton(text='▶️', callback_data=f'medialist_forward={msg.chat.id},{msg.message_id},{mediagroup_id},1'))
                await msg.edit_reply_markup(reply_markup=keyboard)

        return msg
    except BotBlocked:
        delete_for_blocked(user_id, bot_id)
    except ChatNotFound:
        delete_for_blocked(user_id, bot_id)
    except UserDeactivated:
        delete_for_blocked(user_id, bot_id)
    except WrongFileIdentifier:
        slide['message'] = '<b>ERR: WrongFileIdentifier</b>\n\n' + (slide['message'] or '')
        slide['type'] = None
        msg = await construct_slide_from_message(slide, user_id, is_bot_msg)
        return msg
    except Exception as err:
        await group_msg(f'slide_error: slide={slide["id"]}; err - {type(err).__name__}:{err}\nSlide:\n{slide}\n\nKeyboard:\n{keyboard}')
        raise SlideError


async def change_slide(chat_id, message_id, media_type=None, media_id=None, text=None, reply_markup=None):
    try:
        if media_type is None:
            if text is not None:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
            if reply_markup is not None:
                    await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=reply_markup)
        else:
            media = types.InputMedia()
            if media_type == 'gif':
                media = InputMediaAnimation(media=media_id)
            elif media_type == 'document':
                media = InputMediaDocument(media=media_id)
            elif media_type == 'voice':
                media = InputMediaAudio(media=media_id)
            elif media_type == 'photo':
                media = InputMediaPhoto(media=media_id)
            elif media_type == 'video':
                media = InputMediaVideo(media=media_id)
            await bot.edit_message_media(media=media, chat_id=chat_id, message_id=message_id)
            if text is not None:
                await bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=text)
            if reply_markup is not None:
                await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except Exception as err:
        await group_msg(f"change_slide_err: {type(err).__name__}: {err}")
