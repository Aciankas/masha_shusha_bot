import re

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message, InputMediaPhoto, \
    InputMediaVideo, InputMediaAudio, InputMediaAnimation, InputMediaDocument, ParseMode, InputMedia
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, UserDeactivated, WrongFileIdentifier, MessageNotModified

import dtbase
import datetime

from create_bot import bot_id as tlg_bot_id, timezone, bot as tlg_bot, admin_ids, errorstack


class CouponSchedule(Exception):
    match = 'Coupon schedule parsing error'


class TransactionInit(Exception):
    match = 'Transaction should be initialised by id or by (user_id, merch_id)'


class MediaBlockInit(Exception):
    match = 'MediaBlock should be initialised by id or by (media_id, media_type)'


class MediaBlockNotFound(Exception):
    match = 'MediaBlock not found by id in database'


class MediaUnitNotFound(Exception):
    match = 'MediaUnit not found by id in database'


class ButtonNotFound(Exception):
    match = 'Button not found by slide_id, row_num and row_pos in database'


class ButtonArgs(Exception):
    match = 'Not enough arguments to declare Button. Should be row_num, row_pos and name or slide_id at least'


class SlideInit(Exception):
    match = 'Not enough arguments to declare Slide. Should be message, media or slide_id'


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
    return datetime.datetime.combine((datetime.datetime.now() + datetime.timedelta(
        days=week * 7 + (weekday - datetime.datetime.now().weekday() - 1) % 7 + 1)),
                                     datetime.time(hour, minute)) - datetime.timedelta(hours=timezone)


def get_day(day, hour=None, minute=None):
    if hour is None:
        hour = 12
    if minute is None:
        minute = 0
    return datetime.datetime.combine((datetime.datetime.now() + datetime.timedelta(days=day)),
                                     datetime.time(hour, minute)) - datetime.timedelta(hours=timezone)


def get_minutes_from_now(minutes):
    return datetime.datetime.now() + datetime.timedelta(minutes=minutes)


def convert_spec_text(text, user_id=None):  # спец текст находится между %spc%  # old convert_spec_message_text
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
                    text = text.replace(text[open_pos:close_pos],
                                        str(dtbase.spec_to_text(text[open_pos:close_pos], user_id)), 1).replace('%spc%',
                                                                                                                '',
                                                                                                                1).replace(
                        '%/spc%', '', 1)
                else:
                    break
            return text.replace('%spc%', '').replace('%/spc%', '')
    except Exception as err:
        errorstack.add('ERR convert_spec_text: ' + str(err))
        return str(text) + '\n\nconversion_error: ' + str(err)


def any_none(*args):
    for arg in args:
        if arg is None:
            return True
    return False


def all_none(*args):
    for arg in args:
        if arg is not None:
            return False
    return True


def can_appear_by_mod(appearance_mod, user_id=None):  # old slide_button_appears
    try:
        if appearance_mod is None:
            return True
        try:
            appearance = appearance_mod.split('=')
            mod = appearance[0]
            elements = appearance[1].split(",")
        except Exception as err:
            errorstack.add(f"ERR {type(err).__name__}: appearance_mod not correct: {appearance_mod}")
            return True
        if user_id is None or appearance is None:
            return True
        elif mod == 'coupon_expire':
            return dtbase.is_coupon_active(user_id, elements[0])
        elif mod == 'course_paid':
            return dtbase.is_course_paid(user_id, elements[0])
        elif mod == 'course_not_paid':
            return not dtbase.is_course_paid(user_id, elements[0])
        else:
            visited = dtbase.get_slides_from_list_visited(elements, user_id)
            if mod == 'disappear_any':
                return visited == 0
            elif mod == 'disappear_all':
                return visited < len(elements)
            elif mod == 'appear_any':
                return visited > 0
            elif mod == 'appear_all':
                return visited == len(elements)
    except Exception as err:
        errorstack.add(f"ERR {type(err).__name__}: appearance_mod not correct: {appearance_mod}")
        raise


class MediaBlock:
    _mediatypes = ('photo', 'video', 'document', 'gif', 'voice', 'video_note', 'mediagroup', 'medialist')
    _solotypes = ('photo', 'video', 'document', 'gif', 'voice', 'video_note')
    _basictypes = ('photo', 'video', 'document', 'gif', 'voice')
    _grouptypes = ('mediagroup', 'medialist')

    def __init__(self, db_id: int = None, media_id: str or list = None, media_type: str = None):
        self.mediaunits = list()
        if db_id:
            try:
                fetch = dtbase.get_media_by_id(db_id)
            except IndexError:
                raise MediaBlockNotFound
            self.type = fetch["type"]
            if self.type in self._grouptypes:
                mediagroup = dtbase.get_mediagroup(fetch["media_id"])
                for mediaunit in mediagroup:
                    if mediaunit["type"] in self._basictypes:
                        self.mediaunits.append(MediaUnit(media_id=mediaunit["media_id"], media_type=mediaunit["type"]))
                if not self.mediaunits:
                    raise MediaBlockNotFound
            else:
                self.mediaunits.append(MediaUnit(media_id=fetch["media_id"], media_type=fetch["type"]))

        elif media_id and media_type and media_type in self._mediatypes:
            self.type = media_type
            if self.type in self._grouptypes:
                if not isinstance(media_id, list):
                    raise MediaBlockInit
                for mediaunit in media_id:
                    if isinstance(mediaunit, int):
                        try:
                            mediaunit = dtbase.get_media_by_id(db_id)
                        except IndexError:
                            raise MediaBlockNotFound
                        if mediaunit["type"] in self._basictypes:
                            self.mediaunits.append(
                                MediaUnit(media_id=mediaunit["media_id"], media_type=mediaunit["type"]))
                    if isinstance(mediaunit, MediaUnit):
                        if mediaunit.type in self._basictypes:
                            self.mediaunits.append(mediaunit)
                if not self.mediaunits:
                    raise MediaBlockNotFound
            else:
                self.mediaunits.append(MediaUnit(media_id=media_id, media_type=media_type))

        else:
            self.type = 'text'
            self.mediaunits.append(MediaUnit())

    def file_id(self):
        return self.mediaunits[0].media_id

    def unit_type(self):
        return self.mediaunits[0].type


class Coupon:
    def __init__(self, coupon_id: int, user_id: int, name: str = None, effect: str = None, end_slide_id: int = None,
                 end_time: datetime.datetime = None, charges: int = None):
        self.id = coupon_id
        self.user_id = user_id
        self.name = name
        self.effect = effect
        self.end_slide_id = end_slide_id
        self.end_time = end_time
        self.charges = charges
        if any_none(name, effect, end_slide_id, end_time, charges):
            try:
                fetch = dtbase.get_scheduled_coupon(self.id, self.user_id)
                self.name = fetch["name"]
                self.effect = fetch["effect"]  # в dict перевести потом (ниже)
                self.end_slide_id = fetch["end_slide_id"]
                self.end_time = fetch["end_time"]
                self.charges = fetch["charges"]
            except IndexError:
                errorstack.add(f"ERR: Coupon {self.id}/{self.user_id} can`t be found.")
                raise
            except Exception as err:
                errorstack.add(f"ERR: Coupon {self.id}/{self.user_id} unusual error. {type(err).__name__}")
                raise

        try:
            fetch_merch = dtbase.get_coupon_merch_ids(self.id)
            self.merch_ids = list()
            for merch in fetch_merch:
                self.merch_ids.append(merch["id_merch"])
        except IndexError:
            errorstack.add(f"ERR: Coupon.merch_ids {self.id}/{self.user_id} can`t be found.")
            raise
        except Exception as err:
            errorstack.add(f"ERR: Coupon.merch_ids {self.id}/{self.user_id} unusual error. {type(err).__name__}")
            raise


class User:
    def __init__(self, user_id: int, name: str = None, uname: str = None, lastname: str = None):
        self.id = user_id
        self.name = name
        self.uname = uname
        self.lastname = lastname
        self.reg_date = None
        if all_none(name, uname, lastname):
            try:
                fetch = dtbase.get_userdata_by_id(self.id)
                self.name = fetch['name']
                self.uname = fetch['uname']
                self.lastname = fetch['lastname']
                self.reg_date = fetch['reg_date']
            except IndexError:
                errorstack.add(f"ERR: User {self.id} can`t be found.")
                raise
            except Exception as err:
                errorstack.add(f"ERR: User {self.id} unusual error. {type(err).__name__}")
                raise

    def db_register(self):
        dtbase.reg_user(self.id, self.name, self.uname, self.lastname)

    def db_give_coupon(self, coupon_id: int):
        try:
            coupon = dtbase.get_coupon_by_id(coupon_id)
            schedule_parsed = None
            if re.match(r'\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}', coupon["schedule_set"]):
                schedule_parsed = datetime.datetime.strptime(coupon["schedule_set"],
                                                             '%d.%m.%Y %H:%M') - datetime.timedelta(hours=timezone)
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
                dtbase.create_scheduled_coupon(schedule_parsed, self.id, coupon_id, coupon["charges"])
        except Exception:
            raise CouponSchedule

    def get_coupon_list_for_merch(self, merch_id: int):
        coupon_list = list()

        try:
            fetch = dtbase.get_coupons_for_merch_and_user(self.id, merch_id)
            for coupon in fetch:
                coupon_list.append(Coupon(coupon_id=coupon["id"],
                                          user_id=self.id,
                                          name=coupon["name"],
                                          effect=coupon["effect"],
                                          end_slide_id=coupon["end_slide_id"],
                                          end_time=coupon["end_time"],
                                          charges=coupon["charges"]))
        except IndexError:
            pass
        except Exception as err:
            errorstack.add(f"ERR: User.coupon_list_for_merch {self.id} unusual error. {type(err).__name__}")
            raise

        return coupon_list

    def can_coupon_be_used(self, merch_id: int, coupon_id: int):
        return dtbase.can_coupon_be_used(self.id, merch_id, coupon_id)

    def is_course_paid(self, course_id: int):
        return dtbase.is_course_paid(self.id, course_id)

    def create_transaction(self, merch_id: int, media: MediaBlock, coupon_id: int = None):
        dtbase.create_transaction(self.id, merch_id, media.file_id(), media.unit_type(), coupon_id)
        return Transaction(user_id=self.id, merch_id=merch_id, *'processing')


class Course:
    def __init__(self, course_id: int, name: str = None, bot_id: str = None):
        self.id = course_id
        self.name = name
        self.bot_id = bot_id
        if any_none(name, bot_id):
            try:
                fetch = dtbase.get_course_by_id(self.id)
                self.name = fetch["name"]
                self.bot_id = fetch["bot_id"]
            except IndexError:
                errorstack.add(f"ERR Course {self.id} can`t be found.")
                raise
            except Exception as err:
                errorstack.add(f"ERR: Course {self.id} unusual error. {type(err).__name__}")
                raise


class Merchandise:
    def __init__(self, merch_id: int, name: str = None, price: int = None, courses: list = None):
        self.id = merch_id
        self.name = name
        self.price = price
        self.courses = courses
        if any_none(name, price, courses) or not courses:
            try:
                fetch = dtbase.get_merch_by_id(self.id)
                self.name = fetch["name"]
                self.price = fetch["price"]
                fetch_courses = dtbase.get_courses_by_merch_id(self.id)
                self.courses = list()
                for course in fetch_courses:
                    self.courses.append(Course(course_id=course["id"],
                                               name=course["name"],
                                               bot_id=course["bot_id"]))
            except IndexError:
                errorstack.add(f"ERR Course {self.id} can`t be found.")
                raise
            except Exception as err:
                errorstack.add(f"ERR: Course {self.id} unusual error. {type(err).__name__}")
                raise


class MediaUnit:
    _mediatypes = ('photo', 'video', 'document', 'gif', 'voice', 'video_note', 'mediagroup', 'medialist')
    _solotypes = ('photo', 'video', 'document', 'gif', 'voice', 'video_note')
    _basictypes = ('photo', 'video', 'document', 'gif', 'voice')
    _grouptypes = ('mediagroup', 'medialist')

    def __init__(self, db_id: int = None, media_id: str or list = None, media_type: str = None):
        if db_id:
            try:
                fetch = dtbase.get_media_by_id(db_id)
            except IndexError:
                raise MediaUnitNotFound

            if fetch["type"] in self._grouptypes:
                self.__class__ = MediaBlock
                self.__init__(db_id, media_id, media_type)
                return

            self.type = fetch["type"]
            self.media_id = fetch["media_id"]

        elif media_id and media_type and media_type in self._mediatypes:

            if media_type in self._grouptypes:
                self.__class__ = MediaBlock
                self.__init__(db_id, media_id, media_type)
                return

            self.type = media_type
            self.media_id = media_id

        else:
            self.type = 'text'
            self.media_id = None

    def db_create_new(self):
        if self.media_id and self.type and isinstance(self.media_id, str) and self.type in self._solotypes:
            dtbase.insert_media(self.media_id, self.type)


class Transaction:
    _correct_statuses = ('processing', 'commit', 'reject')

    def __init__(self, transaction_id: int = None, user_id: int = None, merch_id: int = None, *statuses):
        if transaction_id is not None:  # transaction by id
            try:
                fetch = dtbase.get_transaction_by_id(transaction_id)
            except IndexError:
                self.__class__ = None
                return
            self.id = transaction_id
            self.user = User(fetch["user_id"])
            self.merch = Merchandise(fetch["merch_id"])
        elif user_id is not None and merch_id is not None and self.are_statuses_correct(
                statuses):  # last transaction from user with merch_id
            try:
                fetch = dtbase.get_transactions(user_id, merch_id, *statuses)[0]
            except IndexError:
                self.__class__ = None
                return
            self.id = fetch["id"]
            self.user = User(user_id)
            self.merch = Merchandise(merch_id)
        else:
            raise TransactionInit

        if fetch["coupon_id"]:
            self.coupon = Coupon(fetch["coupon_id"], self.user.id)
        else:
            self.coupon = None

        self.status = fetch["status"]
        self.media = MediaBlock(media_id=fetch["media_id"], media_type=fetch["type"])

    def db_update_status(self, status: str = None):
        if status in self._correct_statuses:
            self.status = status
        if self.status in self._correct_statuses:
            dtbase.update_transaction_status(self.id, self.status)
            if self.status == 'reject' and self.coupon:
                dtbase.return_coupon(self.user.id, self.coupon.id)

    def are_statuses_correct(self, statuses: tuple):
        for status in statuses:
            if status not in self._correct_statuses:
                return False
        return True


class Button:
    def __init__(self, row_num: int, row_pos: int, slide_id: int = None, slide_link: int = None, name: str = None,
                 url: str = None, modifier: str = None, appearance_mod: str = None):
        if row_num and row_pos and slide_id:
            try:
                button_fetch = dtbase.get_button_by_ids(slide_id, row_num, row_pos)
            except IndexError:
                raise ButtonNotFound
            self.row_num = row_num
            self.row_pos = row_pos
            self.slide_id = slide_id
            self.slide_link = slide_link or button_fetch["slide_link"]
            self.name = name or button_fetch["name"]
            self.url = url or button_fetch["url"]
            self.modifier = modifier or button_fetch["modifier"]
            self.appearance_mod = appearance_mod or button_fetch["appearance_mod"]
        elif row_num and row_pos and name:
            self.row_num = row_num
            self.row_pos = row_pos
            self.slide_id = slide_id
            self.slide_link = slide_link
            self.name = name
            self.url = url
            self.modifier = modifier
            self.appearance_mod = appearance_mod
        else:
            raise ButtonArgs


class Keyboard:
    def __init__(self, *buttons: Button):
        self.buttons = list(buttons)

    def construct(self, user_id: int = None):  # old construct_keyboard_from_select
        self.buttons = sorted(self.buttons, key=lambda x: (x.row_num, x.row_pos))
        if self.buttons:
            try:
                cur_row_list = []
                cur_row = self.buttons[0].row_num
                keyboard_list = []
                for button in self.buttons:
                    if can_appear_by_mod(button.appearance_mod, user_id):
                        if cur_row_list != [] and cur_row != button.row_num:
                            keyboard_list.append(cur_row_list)
                            cur_row = button.row_num
                            cur_row_list = []
                        if button.modifier is not None:
                            cur_row_list.append(InlineKeyboardButton(text=button.name, callback_data=button.modifier))
                        elif button.url is not None:
                            cur_row_list.append(InlineKeyboardButton(text=button.name, url=button.url))
                        else:
                            cur_row_list.append(
                                InlineKeyboardButton(text=button.name, callback_data=f'slide_{button.slide_link}'))
                keyboard_list.append(cur_row_list)
                keyboard = InlineKeyboardMarkup()
                for button in keyboard_list:
                    keyboard.row(*button)
                return keyboard
            except Exception as err:
                raise err
        else:
            return None

    def db_get_buttons(self, slide_id: int):  # old construct_keyboard
        select = dtbase.get_keyboard(slide_id)
        slide_buttons = list()
        for button in select:
            slide_buttons.append(Button(button["row_num"],
                                        button["row_pos"],
                                        button["slide_id"],
                                        button["slide_link"],
                                        button["name"],
                                        button["url"],
                                        button["modifier"],
                                        button["appearance_mod"]))
        self.buttons += slide_buttons
        return self

    def copy(self):
        return Keyboard(*self.buttons)

    def add_buttons(self, *buttons: Button):
        self.buttons += list(buttons)

    def get_button(self, row_num: int, row_pos: int):
        for button in self.buttons:
            if button.row_num == row_num and button.row_pos == row_pos:
                return button


SPECIAL_BUTTON = {'medialist_back': {"row_num": -1, "row_pos": 1},
                  'medialist_forward': {"row_num": -1, "row_pos": 2},
                  'quest_commit': {"row_num": 999, "row_pos": 999}}


class Slide:
    def __init__(self, slide_id: int or str = None, message: str = None, media: MediaBlock = None,
                 keyboard: Keyboard = None, modifier: str = None, appearance_mod: str = None,
                 schedule_set: str = None, schedule_priority: int = None, del_if_blocked: bool = None,
                 telegram_chat_id: int = None, telegram_message_id: int = None):
        self.telegram_chat_id = telegram_chat_id
        self.telegram_message_id = telegram_message_id
        self.user = None
        if slide_id:
            try:
                fetch = dtbase.get_slide(slide_id, tlg_bot_id)
                self.id = fetch["id"]
                self.message = message or fetch["message"]
                self.media = media or MediaBlock(fetch["media_id"])
                self.keyboard = keyboard or Keyboard().db_get_buttons(fetch["id"])
                self.modifier = modifier or fetch["modifier"]
                self.appearance_mod = appearance_mod or fetch["appearance_mod"]
                self.schedule_set = schedule_set or fetch["schedule_set"]
                self.schedule_priority = schedule_priority or fetch["schedule_priority"]
                self.del_if_blocked = del_if_blocked or fetch["del_if_blocked"]
                self.header = fetch["header"]
                self.bot_id = fetch["bot_id"]
            except Exception as err:
                errorstack.add(f'Slide.__init__ \nslide_id: {slide_id}\ntlg_bot_id: {tlg_bot_id}\n{type(err).__name__}\n{err}')
                self.id = -1
                self.message = f'Техническая ошибка.\nslide_id: {slide_id}\ntlg_bot_id: {tlg_bot_id}\n{type(err).__name__}\n{err}'
                self.media = MediaBlock()
                self.keyboard = Keyboard()
                self.modifier = None
                self.appearance_mod = None
                self.schedule_set = None
                self.schedule_priority = 0
                self.del_if_blocked = False
                self.header = 'Ошибка нахождения слайда'
                self.bot_id = tlg_bot_id
        elif message or (media and isinstance(media, MediaBlock)):
            self.id = slide_id
            self.message = message
            self.media = media or MediaBlock()
            self.keyboard = keyboard or Keyboard()
            self.modifier = modifier
            self.appearance_mod = appearance_mod
            self.schedule_set = schedule_set
            self.schedule_priority = schedule_priority or 0
            self.del_if_blocked = del_if_blocked or False
            self.header = None
            self.bot_id = tlg_bot_id
        else:
            raise SlideInit

    def copy(self):
        new = Slide(self.id, self.message, self.media, self.keyboard, self.modifier, self.appearance_mod,
                    self.schedule_set, self.schedule_priority, self.header, self.del_if_blocked)
        new.user = self.user
        new.header = self.header
        return new

    async def deliver_message(self, user_id: int = None, postfix_text: str = None, is_bot_msg: bool = False):
        if user_id:
            self.user = User(user_id)
        if not isinstance(self.user, User):
            errorstack.add(f'Slide.deliver_message user is not defined: slide={self.id}')
            return None
        try:
            if self.id:
                dtbase.click_log(self.user.id, self.id, is_bot_msg)

            msg = Message()
            if self.media.type == 'mediagroup':
                mediagroup = list()
                for mediaunit in self.media.mediaunits:
                    if mediaunit.type == 'photo':
                        mediagroup.append(InputMediaPhoto(mediaunit.media_id))
                    elif mediaunit.type == 'video':
                        mediagroup.append(InputMediaVideo(mediaunit.media_id))
                    elif mediaunit.type == 'voice':
                        mediagroup.append(InputMediaAudio(mediaunit.media_id))
                    elif mediaunit.type == 'gif':
                        mediagroup.append(InputMediaAnimation(mediaunit.media_id))
                    elif mediaunit.type == 'document':
                        mediagroup.append(InputMediaDocument(mediaunit.media_id))
                msg = await tlg_bot.send_media_group(self.user.id,
                                                     mediagroup,
                                                     protect_content=True,
                                                     parse_mode=ParseMode.HTML)
            elif self.media.unit_type() == 'text':
                msg = await tlg_bot.send_message(self.user.id,
                                                 (self.message or '') + (postfix_text or ''),
                                                 protect_content=True,
                                                 reply_markup=self.keyboard.construct(self.user.id),
                                                 parse_mode=ParseMode.HTML)
            elif self.media.unit_type() == 'photo':
                msg = await tlg_bot.send_photo(self.user.id,
                                               self.media.file_id(),
                                               caption=(self.message or '') + (postfix_text or ''),
                                               protect_content=True,
                                               reply_markup=self.keyboard.construct(self.user.id),
                                               parse_mode=ParseMode.HTML)
            elif self.media.unit_type() == 'video':
                msg = await tlg_bot.send_video(self.user.id,
                                               self.media.file_id(),
                                               caption=(self.message or '') + (postfix_text or ''),
                                               protect_content=True,
                                               reply_markup=self.keyboard.construct(self.user.id),
                                               parse_mode=ParseMode.HTML)
            elif self.media.unit_type() == 'document':
                msg = await tlg_bot.send_document(self.user.id,
                                                  self.media.file_id(),
                                                  caption=(self.message or '') + (postfix_text or ''),
                                                  protect_content=True,
                                                  reply_markup=self.keyboard.construct(self.user.id),
                                                  parse_mode=ParseMode.HTML)
            elif self.media.unit_type() == 'gif':
                msg = await tlg_bot.send_animation(self.user.id,
                                                   self.media.file_id(),
                                                   caption=(self.message or '') + (postfix_text or ''),
                                                   protect_content=True,
                                                   reply_markup=self.keyboard.construct(self.user.id),
                                                   parse_mode=ParseMode.HTML)
            elif self.media.unit_type() == 'voice':
                msg = await tlg_bot.send_voice(self.user.id,
                                               self.media.file_id(),
                                               caption=(self.message or '') + (postfix_text or ''),
                                               protect_content=True,
                                               reply_markup=self.keyboard.construct(self.user.id),
                                               parse_mode=ParseMode.HTML)
            elif self.media.unit_type() == 'video_note':
                msg = await tlg_bot.send_video_note(self.user.id,
                                                    self.media.file_id(),
                                                    protect_content=True,
                                                    reply_markup=self.keyboard.construct(self.user.id),
                                                    parse_mode=ParseMode.HTML)
            else:
                errorstack.add(f'Slide.deliver_message media type error: slide={self.id}, mediatype={self.media.unit_type()}')

            self.telegram_message_id = msg.message_id
            self.telegram_chat_id = msg.chat.id
            if self.media.type == 'medialist':
                self.keyboard.add_buttons(Button(row_num=SPECIAL_BUTTON["medialist_back"]["row_num"],
                                                 row_pos=SPECIAL_BUTTON["medialist_back"]["row_pos"],
                                                 name='◀️',
                                                 modifier=f'medialist_back={self.telegram_chat_id},{self.telegram_message_id},{self.id},0'),
                                          Button(row_num=SPECIAL_BUTTON["medialist_forward"]["row_num"],
                                                 row_pos=SPECIAL_BUTTON["medialist_forward"]["row_pos"],
                                                 name='▶️',
                                                 modifier=f'medialist_forward={self.telegram_chat_id},{self.telegram_message_id},{self.id},0'))
                await msg.edit_reply_markup(reply_markup=self.keyboard.construct(self.user.id))
            return msg
        except BotBlocked:
            dtbase.delete_for_blocked(self.user.id, tlg_bot_id)
        except ChatNotFound:
            dtbase.delete_for_blocked(self.user.id, tlg_bot_id)
        except UserDeactivated:
            dtbase.delete_for_blocked(self.user.id, tlg_bot_id)
        except WrongFileIdentifier:
            self.media = MediaBlock()
            self.message = '<b>ERR: WrongFileIdentifier</b>\n\n' + (self.message or '')
            msg = await self.deliver_message(postfix_text=postfix_text, is_bot_msg=is_bot_msg)
            return msg
        except Exception as err:
            errorstack.add(f'Slide.deliver_message: slide={self.id}; err - {type(err).__name__}:{err}')

    async def send(self, user: User, is_bot_msg: bool = False):
        try:
            if not self.telegram_chat_id or not self.telegram_message_id:
                self.user = user
                if can_appear_by_mod(self.appearance_mod, user.id):
                    try:
                        if self.user.id in admin_ids:
                            spec_text = f'\n\n/slide_{self.id}'
                        else:
                            spec_text = ''
                        if self.schedule_set is not None:
                            self.schedule_create(self.schedule_set, self.user.id)
                        self.message = convert_spec_text(self.message, self.user.id)
                        # modifier effects
                        modifier = nvl(self.modifier, 'null')
                        if modifier == 'empty':
                            return
                        if 'redirect' in modifier:
                            try:
                                mod_value = str(self.modifier).split('=')[1]
                                msg = await Slide(mod_value).send(self.user, is_bot_msg)
                            except Exception as err:
                                errorstack.add(f'Slide.send: redirect_error: slide={self.id}; err - {err}')
                            return
                        if 'coupon_start' in modifier:
                            user.db_give_coupon(int(self.modifier.split('_')[2]))  # await coupon_schedule_create(self.user.id, int(slide["modifier"].split('_')[2]))
                        if 'course_price' in modifier:
                            mod_value = int(self.modifier.split('_')[2])
                            coupons = user.get_coupon_list_for_merch(
                                mod_value)  # get_coupons_for_merch(self.user.id, int(slide["modifier"].split('_')[2]))
                            for idx, coupon in enumerate(coupons, 901):
                                self.keyboard.buttons.append(Button(idx, 990, coupon.name,
                                                                    modifier=f'coupon_use={mod_value},{coupon.id}'))  # keyboard.row(InlineKeyboardButton(text=coupon["name"], callback_data=f'coupon_use={slide["modifier"].split("_")[2]},{coupon["id"]}'))
                        return await self.deliver_message(postfix_text=spec_text, is_bot_msg=is_bot_msg)

                    except Exception as err:
                        errorstack.add(f'ERR: Slide.send - {type(err).__name__}/{err}')

            else:
                errorstack.add('Slide is already sent. Can`t send. Updating.')
                await self.update()
        except Exception as err:
            errorstack.add(f'ERR Critical! Slide.send - {type(err).__name__}\nUser = {user}\nSlide = {self}')

    async def answer(self, msg: CallbackQuery or Message, is_bot_msg: bool = False):
        try:
            aiogram_user = msg.from_user
            try:
                if not self.user:
                    self.user = User(aiogram_user.id)
            except IndexError:
                User(user_id=aiogram_user.id,
                     name=aiogram_user.first_name,
                     uname=aiogram_user.username,
                     lastname=aiogram_user.last_name).db_register()
                self.user = User(aiogram_user.id)
            await self.send(self.user, is_bot_msg=is_bot_msg)
        except Exception as err:
            errorstack.add(f'ERR Critical! Slide.answer - {type(err).__name__}\nUser = {msg.from_user}\nSlide = {self}')

    async def update(self):
        try:
            if self.telegram_chat_id and self.telegram_message_id:
                try:
                    errorstack.add(f"self.media.unit_type() = {self.media.unit_type()}")  # LOGS
                    if self.media.unit_type() == 'text':
                        if self.message:
                            await tlg_bot.edit_message_text(chat_id=self.telegram_chat_id,
                                                            message_id=self.telegram_message_id, text=self.message)
                        if self.keyboard:
                            await tlg_bot.edit_message_reply_markup(chat_id=self.telegram_chat_id,
                                                                    message_id=self.telegram_message_id,
                                                                    reply_markup=self.keyboard.construct())
                    else:
                        media = InputMedia()
                        if self.media.unit_type() == 'gif':
                            media = InputMediaAnimation(media=self.media.file_id())
                        elif self.media.unit_type() == 'document':
                            media = InputMediaDocument(media=self.media.file_id())
                        elif self.media.unit_type() == 'voice':
                            media = InputMediaAudio(media=self.media.file_id())
                        elif self.media.unit_type() == 'photo':
                            media = InputMediaPhoto(media=self.media.file_id())
                        elif self.media.unit_type() == 'video':
                            media = InputMediaVideo(media=self.media.file_id())
                        await tlg_bot.edit_message_media(media=media, chat_id=self.telegram_chat_id,
                                                         message_id=self.telegram_message_id)
                        if self.message:
                            await tlg_bot.edit_message_caption(chat_id=self.telegram_chat_id,
                                                               message_id=self.telegram_message_id,
                                                               caption=self.message)
                        if self.keyboard:
                            await tlg_bot.edit_message_reply_markup(chat_id=self.telegram_chat_id,
                                                                    message_id=self.telegram_message_id,
                                                                    reply_markup=self.keyboard.construct())
                except MessageNotModified:
                    pass
                except Exception as err:
                    errorstack.add(f"change_slide_err: {type(err).__name__}: {err}")
            else:
                errorstack.add('ERR: Slide is not even sent. Can`t change.')
        except Exception as err:
            errorstack.add(f'ERR Critical! Slide.answer - {type(err).__name__}\nUser = {self.user}\nSlide = {self}')

    def move_medialist(self, decision: str, medialist_num: int):
        if self.media.type == 'medialist' and decision in ('back', 'forward'):
            media_num = 0
            if decision == 'back':
                media_num = (medialist_num - 1 + len(self.media.mediaunits)) % len(self.media.mediaunits)
            elif decision == 'forward':
                media_num = (medialist_num + 1 + len(self.media.mediaunits)) % len(self.media.mediaunits)
            self.keyboard.get_button(SPECIAL_BUTTON["medialist_back"]["row_num"],
                                     SPECIAL_BUTTON["medialist_back"]["row_pos"])\
                .modifier = f'medialist_back={self.telegram_chat_id},{self.telegram_message_id},{self.id},{media_num}'
            self.keyboard.get_button(SPECIAL_BUTTON["medialist_forward"]["row_num"],
                                     SPECIAL_BUTTON["medialist_forward"]["row_pos"])\
                .modifier = f'medialist_forward={self.telegram_chat_id},{self.telegram_message_id},{self.id},{media_num}'
            self.media.mediaunits = [self.media.mediaunits[media_num]]

    @staticmethod
    def schedule_create(string, user_id):  # 'weekday=0,week=0,hour=13,minute=30,slide=2;day=2,hour=10,minute=20,slide=3;minutes=30,slide=4'
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
                    dtbase.create_scheduled(schedule_parsed, user_id, int(schedule['slide']))
            except Exception as err:
                errorstack.add(f'ERR: Slide.schedule_create {type(err).__name__}/{err}')

    def send_to_group(self, group: list):
        for user_id in group:
            self.send(User(user_id))


class Question:
    def __init__(self, quest_id: int, user: User):
        self.user = user
        self.slide = None
        self.orig_keyboard = None
        self.create_quest_slide(int(dtbase.get_questionnaire_start_slide))
        self.finish_slide_id = int(dtbase.get_questionnaire_finish_slide(quest_id))
        self.answer = None

    def is_ever_completed(self):
        return int(dtbase.get_slides_from_list_visited([self.finish_slide_id], self.user.id)) > 0

    def create_quest_slide(self, slide_id: int):
        self.slide = Slide(slide_id)
        for button in self.slide.keyboard.buttons:
            button.modifier = f"answer={button.row_num},{button.row_pos};slide={button.slide_id}"
        self.orig_keyboard = self.slide.keyboard.copy()

    def quest_type(self):
        if len(self.slide.keyboard.buttons) == 0:
            return 'text'
        elif self.slide.modifier == 'quest_multiple':
            return 'multiple'
        else:
            return 'buttons'

    async def send(self):
        self.slide.send(self.user)

    async def send_next(self):
        next_id = int(dtbase.get_questionnaire_next_slide(self.slide.id))
        if next_id != self.finish_slide_id:
            self.create_quest_slide(next_id)
        else:
            self.slide = Slide(next_id)
        await self.send()

    def commit_answer(self):
        if isinstance(self.answer, str):
            dtbase.set_questionnaire_answer(self.slide.id, self.user.id, self.answer)
            return {"success": True}

        if isinstance(self.answer, Button):
            dtbase.set_questionnaire_answer(self.slide.id, self.user.id, self.answer.name)
            return {"success": True}

        if isinstance(self.answer, list):
            if not self.answer:
                return {"success": False, "err_message": "Необходимо выбрать минимум один вариант ответа"}
            for single in self.answer:
                if not isinstance(single, Button):
                    return {"success": False, "err_message": "Внутренняя ошибка записи множественного ответа (ca)"}
                dtbase.set_questionnaire_answer(self.slide.id, self.user.id, single.name)
                return {"success": True}

        self.answer = None

    async def give_answer(self, answer: str or Button):
        if self.quest_type() == 'text' and isinstance(answer, str):
            self.answer = answer
            return {"success": True}

        if self.quest_type() == 'buttons' and isinstance(answer, Button):
            if answer.slide_id != self.slide.id:
                return {"success": False, "err_message": "Выберите ответ из списка своего вопроса"}
            self.answer = answer
            return {"success": True}

        if self.quest_type() == 'multiple' and isinstance(answer, Button):
            if answer.slide_id != self.slide.id:
                return {"success": False, "err_message": "Выберите ответ из списка своего вопроса"}
            if self.answer:
                if isinstance(self.answer, list):
                    not_in_list = True
                    for idx, single in enumerate(self.answer):
                        if single.row_num == answer.row_num and single.row_pos == answer.row_pos:
                            not_in_list = False
                            self.answer.pop(idx)
                            break
                    if not_in_list:
                        self.answer.append(self.answer)

                return {"success": False, "err_message": "Внутренняя ошибка записи множественного ответа (ga)"}
            else:
                self.answer = [answer]

            self.slide.keyboard = self.orig_keyboard.copy()
            for single in self.answer:
                for button in self.slide.keyboard.buttons:
                    if int(button.row_num) == int(single.row_num) and int(button.row_pos) == int(single.row_pos):
                        button.name = f"✅{button.name}"
            self.slide.keyboard.buttons.append(
                Button(row_num=SPECIAL_BUTTON['quest_commit']['row_num'],
                       row_pos=SPECIAL_BUTTON['quest_commit']['row_pos'],
                       name='Отправить ответы',
                       modifier='quest_commit'))
            await self.slide.update()

            return {"success": True}

        return {"success": False, "err_message": "Введите корректный ответ"}

    def answer_as_text(self):
        if isinstance(self.answer, str):
            return self.answer

        if isinstance(self.answer, Button):
            return self.answer.name

        if isinstance(self.answer, list):
            text = str()
            for single in self.answer:
                if isinstance(single, Button):
                    text += f"{single.name}\n"
            return text
