from aiogram import types
from aiogram.dispatcher.filters import Text
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from create_bot import dp, bot_id, group_msg, moderator_ids, delete_message
from dtbase import reg_user, get_slide, get_course_by_id, transaction_create, get_transactions, get_transaction_by_id, \
    update_transaction_status, get_questionnaire_start_slide, get_questionnaire_next_slide, \
    set_questionnaire_answer, user_active_slides, can_coupon_be_used, get_coupon_by_id, coupon_use, coupon_return, \
    get_medialist_cnt, get_userdata_by_id, get_start_arg, slide_has_buttons, get_button_by_ids, \
    questionnaire_multiple_commit
from message_constructor import construct_slide, SlideError, construct_slide_from_message, convert_spec_message_text, \
    change_slide, construct_keyboard


class FSMPayment(StatesGroup):
    course = State()
    bill = State()


class FSMQuest(StatesGroup):
    quest_text = State()
    quest_button = State()
    quest_multiple = State()


async def modifier_effects(slide):
    if slide['modifier'] is not None:
        modifier = str(slide['modifier']).split('=')
        mod_name = modifier[0]
        if len(modifier) == 2:
            mod_value = modifier[1]
        else:
            mod_value = None
        if mod_name == 'payment_start':
            await FSMPayment.course.set()


async def get_started(message: types.Message):
    reg_user(message.from_user.id, message.from_user.first_name,
             message.from_user.username, message.from_user.last_name)
    arguments_data = message.get_args()
    if arguments_data:
        try:
            slide = get_slide(get_start_arg(arguments_data, bot_id), bot_id)
        except IndexError:
            slide = get_slide('start', bot_id)
        except Exception as err:
            await group_msg(f"get_started argement error! {err}")
    else:
        slide = get_slide('start', bot_id)
        await modifier_effects(slide)
    try:
        await construct_slide(slide, message)
    except SlideError:
        await message.answer("Что-то пошло не так")
        await group_msg("get_started error!")
    await delete_message(message)


@dp.callback_query_handler(text="payment_refuse", state=FSMPayment.course)
@dp.callback_query_handler(text="payment_refuse", state=FSMPayment.bill)
async def payment_cancel(callback: types.CallbackQuery, state: FSMContext):
    cur_state = await state.get_state()
    if cur_state is None:
        return
    await state.finish()
    await callback.answer('Платёж отменён', show_alert=True)


@dp.callback_query_handler(text_startswith="course_pay", state=FSMPayment.course)
async def payment_course(callback: types.CallbackQuery, state: FSMContext):
    course_id = callback.data.split('=')[1]
    if not get_transactions(callback.from_user.id, course_id, 'processing', 'commit'):
        try:
            async with state.proxy() as data:
                data['course'] = course_id
                data['coupon'] = None
            await construct_slide(get_slide(f'course_price_{course_id}', bot_id), callback)
            await FSMPayment.bill.set()
        except SlideError:
            await callback.answer("Что-то пошло не так")
            await group_msg('payment_course error!')
    else:
        await callback.answer('Данный курс уже был приобретён или транзакция в процессе подтверждения', show_alert=True)


async def payment_document_handler(message: types.Message, state: FSMContext):
    try:
        await construct_slide(get_slide('correct_payment_file', bot_id), message)
        content = None
        if message.content_type == 'photo':
            content = message.photo[0].file_id
        elif message.content_type == 'document':
            content = message.document.file_id
        async with state.proxy() as data:
            course_id = data['course']
            coupon_id = data['coupon']
        course = get_course_by_id(course_id)
        coupon = None
        if coupon_id is not None:
            coupon = get_coupon_by_id(coupon_id)
        transaction_create(message.from_user.id, course_id, content, message.content_type, coupon_id)
        cur_trans = get_transactions(message.from_user.id, course_id, 'processing')[0]["id"]
        msg_text = f'Квитанция по курсу {course["name"]} от пользователя @{message.from_user.username}'
        if coupon is None:
            msg_text += f". Стоимость: {course['price']}₽"
        else:
            msg_text += convert_spec_message_text(f". Использован купон {coupon['name']}. Итоговая стоимость: %spc%course_price_with_coupon={course_id},{coupon_id}%/spc%₽")
        await group_msg(msg_text,
                        moderator_ids, message.content_type, content,
                        InlineKeyboardMarkup().row(InlineKeyboardButton(text='Подтвердить',
                                                                        callback_data=f'commit_payment={cur_trans}'),
                                                   InlineKeyboardButton(text='Отклонить',
                                                                        callback_data=f'reject_payment={cur_trans}')
                                                   ))
    except SlideError:
        await message.answer("Что-то пошло не так")
        await group_msg("payment_document_handler error!")
    await state.finish()


@dp.callback_query_handler(text_startswith="reject_payment")
@dp.callback_query_handler(text_startswith="commit_payment")
async def payment_moderator_commit(callback: types.CallbackQuery):
    if callback.from_user.id in moderator_ids:
        decision = callback.data.split("_")[0]
        trans_id = int(callback.data.split("=")[1])
        cur_trans = get_transaction_by_id(trans_id)
        if cur_trans["status"] == 'processing':
            update_transaction_status(str(trans_id), decision)
            if decision == 'reject':
                await callback.answer(f"Платёж отклонён. @{cur_trans['username']}, Курс: {cur_trans['course_name']}")
                try:
                    coupon_return(cur_trans['user_id'], cur_trans['coupon_id'])
                    slide = get_slide(f"course_reject_{cur_trans['course_id']}", bot_id)
                    await construct_slide_from_message(slide, cur_trans['user_id'], is_bot_msg=True)
                except Exception as err:
                    await group_msg(f"SlideError: course_reject_{cur_trans['course_id']}, {err}")
            elif decision == 'commit':
                await callback.answer(f"Платёж подтверждён. @{cur_trans['username']}, Курс: {cur_trans['course_name']}")
                try:
                    slide = get_slide(f"course_paid_{cur_trans['course_id']}", bot_id)
                    await construct_slide_from_message(slide, cur_trans['user_id'], is_bot_msg=True)
                except Exception as err:
                    await group_msg(f"SlideError: course_paid_{cur_trans['course_id']}, {err}")
        elif cur_trans["status"] == 'commit':
            await callback.answer('Уже оплачено')
        elif cur_trans["status"] == 'reject':
            await callback.answer('Уже отказано')


@dp.callback_query_handler(text_startswith="medialist_back")
@dp.callback_query_handler(text_startswith="medialist_forward")
async def payment_moderator_commit(callback: types.CallbackQuery):
    decision = callback.data.split("=")[0].split("_")[1]
    chat_id = int(callback.data.split("=")[1].split(",")[0])
    message_id = int(callback.data.split("=")[1].split(",")[1])
    mediagroup_id = int(callback.data.split("=")[1].split(",")[2])
    media_count_id = int(callback.data.split("=")[1].split(",")[3])
    new_media = None
    if decision == "back":
        while new_media is None:
            media_count_id = media_count_id - 1
            if media_count_id == 0:
                media_count_id = 10
            try:
                new_media = get_medialist_cnt(mediagroup_id, media_count_id)
            except IndexError:
                pass
            except Exception as err:
                await group_msg(f"Medialist callback: {err}")
    elif decision == "forward":
        while new_media is None:
            media_count_id = media_count_id + 1
            if media_count_id == 10:
                media_count_id = 1
            try:
                new_media = get_medialist_cnt(mediagroup_id, media_count_id)
            except IndexError:
                pass
            except Exception as err:
                await group_msg(f"Medialist callback: {err}")
    keyboard = InlineKeyboardMarkup().row(
        InlineKeyboardButton(text='◀️',
                             callback_data=f'medialist_back={chat_id},{message_id},{mediagroup_id},{media_count_id}'),
        InlineKeyboardButton(text='▶️',
                             callback_data=f'medialist_forward={chat_id},{message_id},{mediagroup_id},{media_count_id}'))
    await change_slide(chat_id=chat_id,
                       message_id=message_id,
                       media_type=new_media["type"],
                       media_id=new_media["media_id"],
                       reply_markup=keyboard)
    await callback.answer()


async def payment_non_document_handler(message: types.Message):
    try:
        await construct_slide(get_slide('incorrect_payment_file', bot_id), message)
    except SlideError:
        await message.answer("Что-то пошло не так")
        await group_msg("payment_non_document_handler error!")


@dp.callback_query_handler(text_startswith="coupon_use", state=FSMPayment.bill)
async def coupon_used(callback: types.CallbackQuery, state: FSMContext):
    args = callback.data.split('=')[1].split(',')
    if can_coupon_be_used(callback.from_user.id, args[0], args[1]):
        await callback.answer(convert_spec_message_text(f'С учётом купона цена составляет %spc%course_price_with_coupon={args[0]},{args[1]}%/spc%₽'), show_alert=True)
        async with state.proxy() as data:
            data['coupon'] = args[1]
    else:
        await callback.answer(convert_spec_message_text(f'Купон истёк или неактивен для данного курса'), show_alert=True)


@dp.callback_query_handler(state=FSMPayment.course)
@dp.callback_query_handler(state=FSMPayment.bill)
async def payment_block(callback: types.CallbackQuery):
    await callback.answer('Необходимо отменить или завершить оплату', show_alert=True)


@dp.callback_query_handler(text_startswith="payment_start")
async def callback_slide_by_modifier(callback: types.CallbackQuery):
    slide = get_slide(callback.data, bot_id)
    try:
        await construct_slide(slide, callback)
        await modifier_effects(slide)
    except SlideError:
        await callback.answer("Что-то пошло не так")
        await group_msg("callback_slide_by_modifier error!")
    await callback.answer()


@dp.callback_query_handler(text_startswith="paycheck=")
async def paycheck_slides(callback: types.CallbackQuery):
    modifiers = callback.data.split('=')[1].split(',')
    course_id = modifiers[0]
    accept_slide = int(modifiers[1])
    decline_slide = int(modifiers[2])
    if get_transactions(callback.from_user.id, course_id, 'commit'):
        slide = get_slide(accept_slide, bot_id)
    else:
        slide = get_slide(decline_slide, bot_id)
    await construct_slide(slide, callback, is_bot_msg=True)
    await callback.answer()


async def questionnaire_state_by_slide_id(slide):
    if slide_has_buttons(slide["id"]):
        if slide["modifier"] == "quest_multiple":
            await FSMQuest.quest_multiple.set()
        else:
            slide["modifier"] = "quest_buttons"
            await FSMQuest.quest_button.set()
    else:
        await FSMQuest.quest_text.set()


@dp.callback_query_handler(text_startswith="questionnaire")
async def questionnaire_start(callback: types.CallbackQuery, state: FSMContext):
    try:
        questionnaire_times = user_active_slides([callback.data.split('=')[1].split(',')[1]], callback.from_user.id)
    except IndexError:  # Если модификатора нет, то опрос многоразовый
        questionnaire_times = 0
    if questionnaire_times == 0:
        slide_id = get_questionnaire_start_slide(callback.data.split('=')[1].split(',')[0])
        slide = get_slide(slide_id, bot_id)
        try:
            await questionnaire_state_by_slide_id(slide)
            slide_appeared = await construct_slide(slide, callback)
            if slide_appeared:
                async with state.proxy() as data:
                    data['slide'] = get_questionnaire_next_slide(slide_id)
                    data['message'] = slide_appeared
                    try:
                        type(data['confirmed_answers'])
                    except KeyError:
                        data['confirmed_answers'] = []
            else:
                await state.finish()
        except SlideError:
            await callback.answer("Что-то пошло не так")
            await group_msg("questionnaire_start error!")
    await callback.answer()


async def questionnaire_results_send(slide_id, user_answered, answer_text, send_ids=moderator_ids):
    try:
        user_tag = get_userdata_by_id(user_answered.id)
    except Exception as err:
        await group_msg(f"ERR: get_userdata_by_id - {type(err).__name__}. User - {user_answered}")
        reg_user(user_answered.id, user_answered.first_name, user_answered.username, user_answered.last_name)
        try:
            user_tag = get_userdata_by_id(user_answered.id)
        except Exception as err:
            await group_msg(f"FATAL ERR: get_userdata_by_id - {type(err).__name__}. User - {user_answered}")
    question_slide = get_slide(int(slide_id), bot_id)
    await group_msg(text=f"Пользователь {user_tag['name']} {user_tag['uname']} {user_tag['lastname']} ответил на вопрос:\n{question_slide['message']}\n\n{answer_text}", usr_ids=send_ids)


async def questionnaire_start_blocker(message: types.Message, state: FSMContext):
    try:
        async with state.proxy() as data:
            cur_slide = data['slide']
        await message.answer("Необходимо закончить предыдущий опрос")
        await construct_slide(get_slide(cur_slide["slide_prev"], bot_id), message)
    except SlideError:
        await message.answer("Что-то пошло не так")
        await group_msg("questionnaire_answer error!")


async def questionnaire_answer(message: types.Message, state: FSMContext):
    try:
        async with state.proxy() as data:
            cur_slide = data['slide']
            # Ответ на предыдущий слайд сюда:
        await questionnaire_results_send(cur_slide["slide_prev"], message.from_user, message.text)
        set_questionnaire_answer(cur_slide["slide_prev"], message.from_user.id, message.text)
        slide = get_slide(cur_slide["slide_id"], bot_id)
        await questionnaire_state_by_slide_id(slide)
        slide_appeared = await construct_slide(slide, message)
        async with state.proxy() as data:
            if len(cur_slide["modifier"].split(".")) > 1 and cur_slide["modifier"].split(".")[1] == 'end':
                await state.finish()
            else:
                data["slide"] = get_questionnaire_next_slide(cur_slide['slide_id'])
                data['message'] = slide_appeared
    except SlideError:
        await message.answer("Что-то пошло не так")
        await group_msg("questionnaire_answer error!")


@dp.callback_query_handler(text_startswith="answer=", state=FSMQuest.quest_button)
async def questionnaire_button_answer(callback: types.CallbackQuery, state: FSMContext):
    try:
        answer = callback.data.split(';')
        button_positions = answer[0].split('=')[1].split(',')
        button_row_num = button_positions[0]
        button_row_pos = button_positions[1]
        answer_slide_id = answer[1].split('=')[1]
        answer_text = get_button_by_ids(answer_slide_id, button_row_num, button_row_pos)["name"]
        async with state.proxy() as data:
            cur_slide = data['slide']
        # Ответ на предыдущий слайд сюда:
        if answer_slide_id == cur_slide["slide_prev"]:
            await questionnaire_results_send(cur_slide["slide_prev"], callback.from_user, answer_text)
            set_questionnaire_answer(cur_slide["slide_prev"], callback.from_user.id, answer_text)
            slide = get_slide(cur_slide["slide_id"], bot_id)
            await questionnaire_state_by_slide_id(slide)
            slide_appeared = await construct_slide(slide, callback)
            async with state.proxy() as data:
                if len(cur_slide["modifier"].split(".")) > 1 and cur_slide["modifier"].split(".")[1] == 'end':
                    await state.finish()
                else:
                    data["slide"] = get_questionnaire_next_slide(cur_slide['slide_id'])
                    data['message'] = slide_appeared
            await callback.answer()
        else:
            await callback.answer('Выберите ответ из списка своего вопроса')
    except SlideError:
        await callback.answer("Что-то пошло не так")
        await group_msg("questionnaire_button_answer error!")


@dp.callback_query_handler(text_startswith="answer=", state=FSMQuest.quest_multiple)
async def questionnaire_multiple_answer(callback: types.CallbackQuery, state: FSMContext):
    try:
        async with state.proxy() as data:
            cur_slide = data['slide']
            cur_msg = data['message']
        answer = callback.data.split(';')
        button_positions = answer[0].split('=')[1].split(',')
        button_row_num = button_positions[0]
        button_row_pos = button_positions[1]
        answer_slide_id = answer[1].split('=')[1]
        applied_answers = []
        if answer_slide_id == cur_slide["slide_prev"]:
            async with state.proxy() as data:
                if {"row_num": button_row_num, "row_pos": button_row_pos} in data['confirmed_answers']:
                    data['confirmed_answers'].remove({"row_num": button_row_num, "row_pos": button_row_pos})
                else:
                    data['confirmed_answers'].append({"row_num": button_row_num, "row_pos": button_row_pos})
                applied_answers = data['confirmed_answers']
        else:
            await callback.answer('Выберите ответ из списка своего вопроса')
        keyboard = await construct_keyboard(cur_slide['slide_prev'], modifier='quest_multiple', applied_answers=applied_answers)
        await change_slide(cur_msg.chat.id, cur_msg.message_id, reply_markup=keyboard)
        await callback.answer()
    except SlideError:
        await callback.answer("Что-то пошло не так")
        await group_msg("questionnaire_button_answer error!")


@dp.callback_query_handler(text_startswith="quest_commit", state=FSMQuest.quest_multiple)
async def questionnaire_multiple_apply(callback: types.CallbackQuery, state: FSMContext):
    try:
        async with state.proxy() as data:
            cur_slide = data['slide']
            confirmed_answers = data['confirmed_answers']
        if len(confirmed_answers) != 0:
            answer_text = ''
            for answer_button in confirmed_answers:
                answer_text += get_button_by_ids(cur_slide['slide_prev'], answer_button["row_num"], answer_button["row_pos"])['name'] + '\n'
            await questionnaire_results_send(cur_slide["slide_prev"], callback.from_user, answer_text)
            questionnaire_multiple_commit(cur_slide['slide_prev'], callback.from_user.id, confirmed_answers)
            # Новый слайд
            slide = get_slide(cur_slide["slide_id"], bot_id)
            await questionnaire_state_by_slide_id(slide)
            slide_appeared = await construct_slide(slide, callback)
            async with state.proxy() as data:
                data['confirmed_answers'] = []
                if len(cur_slide["modifier"].split(".")) > 1 and cur_slide["modifier"].split(".")[1] == 'end':
                    await state.finish()
                else:
                    data["slide"] = get_questionnaire_next_slide(cur_slide['slide_id'])
                    data['message'] = slide_appeared
            await callback.answer()
        else:
            await callback.answer('Необходимо выбрать минимум один вариант ответа')
    except SlideError:
        await callback.answer("Что-то пошло не так")
        await group_msg("questionnaire_multiple_apply error!")


@dp.callback_query_handler(state=FSMQuest.quest_text)
@dp.callback_query_handler(state=FSMQuest.quest_button)
@dp.callback_query_handler(state=FSMQuest.quest_multiple)
async def questionnaire_block(callback: types.CallbackQuery):
    await callback.answer('Необходимо завершить опрос', show_alert=True)


@dp.callback_query_handler(text_startswith="slide_")
async def callback_slide(callback: types.CallbackQuery):
    try:
        split = callback.data.split('_')
        if split[1] != '':
            slide = get_slide(split[1], bot_id)
            try:
                await construct_slide(slide, callback)
                await modifier_effects(slide)
            except SlideError:
                await callback.answer("Что-то пошло не так")
                await group_msg("callback_slide error!")
        await callback.answer()
    except Exception as err:
        await group_msg(f"callback_slide_handler error! {err}")
        await callback.answer()


def register_handlers_client():
    dp.register_message_handler(get_started, commands=['start'])
    dp.register_message_handler(payment_document_handler, state=FSMPayment.bill, content_types=['photo', 'document'])
    dp.register_message_handler(payment_non_document_handler, state=FSMPayment.bill, content_types=['video', 'voice', 'video_note', 'animation'])
    dp.register_message_handler(questionnaire_start_blocker, Text(startswith='/start', ignore_case=True), state=FSMQuest.quest_text)
    dp.register_message_handler(questionnaire_start_blocker, Text(startswith='/start', ignore_case=True), state=FSMQuest.quest_button)
    dp.register_message_handler(questionnaire_answer, state=FSMQuest.quest_text, content_types=types.ContentType.TEXT)
