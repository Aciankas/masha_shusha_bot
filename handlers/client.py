from aiogram import types
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

import dtbase
from create_bot import dp, bot_id, moderator_ids, delete_message, errorstack
from entities import Slide, User, Transaction, Merchandise, Coupon, MediaBlock, Keyboard, Button, Question, \
    convert_spec_text


class FSMPayment(StatesGroup):
    course = State()
    bill = State()


class FSMQuest(StatesGroup):
    quest_text = State()
    quest_button = State()
    quest_multiple = State()


async def state_modifier(mod):
    if mod is not None:
        modifier = str(mod).split('=')
        mod_name = modifier[0]
        if mod_name == 'payment_start':
            await FSMPayment.course.set()


async def get_started(message: types.Message):
    arguments_data = message.get_args()
    slide = Slide('start')
    if arguments_data:
        try:
            slide = Slide(dtbase.get_start_arg(arguments_data, bot_id))
        except IndexError:
            pass
        except Exception as err:
            errorstack.add(f"ERR: get_started argument {type(err).__name__} / {err}")
    try:
        await slide.answer(message)
    except Exception as err:
        await message.answer("Что-то пошло не так")
        errorstack.add(f"ERR: get_started {type(err).__name__} / {err}")
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
@dp.callback_query_handler(text_startswith="merch_pay", state=FSMPayment.course)
async def payment_course(callback: types.CallbackQuery, state: FSMContext):
    merch = Merchandise(int(callback.data.split('=')[1]))

    if Transaction(user_id=callback.from_user.id, merch_id=int(merch.id), *'processing', *'commit'):
        await callback.answer('Данный курс уже был приобретён или транзакция в процессе подтверждения', show_alert=True)
        return

    try:
        async with state.proxy() as data:
            data['merch'] = merch
            data['coupon'] = None
        await Slide(f'course_price_{merch.id}').answer(callback)
        await FSMPayment.bill.set()
    except Exception as err:
        await callback.answer("Что-то пошло не так")
        errorstack.add(f"ERR: payment_course {type(err).__name__} / {err}")


async def payment_document_handler(message: types.Message, state: FSMContext):
    try:
        await Slide('correct_payment_file').answer(message)

        content = None
        if message.content_type == 'photo':
            content = MediaBlock(media_id=message.photo[0].file_id, media_type='photo')
        elif message.content_type == 'document':
            content = MediaBlock(media_id=message.document.file_id, media_type='document')

        async with state.proxy() as data:
            merch = data['merch']
            coupon = data['coupon']

        user = User(message.from_user.id)
        cur_trans = user.create_transaction(merch.id, content, coupon.id)
        msg_text = f'Квитанция по товару {merch.name} от пользователя @{user.uname} / {user.name} {user.lastname}.\n'
        if coupon is None:
            msg_text += f"Стоимость: {merch.price}₽"
        else:
            msg_text += convert_spec_text(f"Использован купон {coupon.name}. Итоговая стоимость: %spc%course_price_with_coupon={merch.id},{coupon.id}%/spc%₽")

        await Slide(message=msg_text,
                    media=content,
                    keyboard=Keyboard(Button(row_num=-1,
                                             row_pos=1,
                                             name='Подтвердить',
                                             modifier=f'commit_payment={cur_trans.id}'),
                                      Button(row_num=-1,
                                             row_pos=2,
                                             name='Отклонить',
                                             modifier=f'reject_payment={cur_trans.id}'))).send_to_group(moderator_ids)
    except Exception as err:
        await message.answer("Что-то пошло не так")
        errorstack.add(f"ERR: payment_document_handler {type(err).__name__} / {err}")
    await state.finish()


@dp.callback_query_handler(text_startswith="reject_payment")
@dp.callback_query_handler(text_startswith="commit_payment")
async def payment_moderator_commit(callback: types.CallbackQuery):
    if callback.from_user.id in moderator_ids:
        transaction = Transaction(transaction_id=int(callback.data.split("=")[1]))

        if transaction.status == 'processing':
            transaction.db_update_status(status=callback.data.split("_")[0])

            if transaction.status == 'reject':
                await callback.answer(f"Платёж отклонён. @{transaction.user.uname}, Товар: {transaction.merch.name}")
                try:
                    await Slide(f"course_reject_{transaction.merch.id}").send(transaction.user, is_bot_msg=True)
                except Exception as err:
                    errorstack.add(f"ERR: course_reject_{transaction.merch.id}, {err}")

            elif transaction.status == 'commit':
                await callback.answer(f"Платёж подтверждён. @{transaction.user.uname}, Товар: {transaction.merch.name}")
                try:
                    await Slide(f"course_paid_{transaction.merch.id}").send(transaction.user, is_bot_msg=True)
                except Exception as err:
                    errorstack.add(f"ERR: course_paid_{transaction.merch.id}, {err}")

        elif transaction.status == 'commit':
            await callback.answer('Уже оплачено')
        elif transaction.status == 'reject':
            await callback.answer('Уже отказано')


@dp.callback_query_handler(text_startswith="medialist_back")
@dp.callback_query_handler(text_startswith="medialist_forward")
async def medialist_moving(callback: types.CallbackQuery):
    slide = Slide(slide_id=int(callback.data.split("=")[1].split(",")[2]),
                  telegram_message_id=int(callback.data.split("=")[1].split(",")[1]),
                  telegram_chat_id=int(callback.data.split("=")[1].split(",")[0]))

    slide.move_medialist(decision=callback.data.split("=")[0].split("_")[1],
                         medialist_num=int(callback.data.split("=")[1].split(",")[3]))

    await slide.update()
    await callback.answer()


async def payment_non_document_handler(message: types.Message, state: FSMContext):
    try:
        await Slide('incorrect_payment_file').answer(message)
    except Exception as err:
        await message.answer("Что-то пошло не так")
        await state.finish()
        errorstack.add(f"ERR: payment_non_document_handler {type(err).__name__} / {err}")


@dp.callback_query_handler(text_startswith="coupon_use", state=FSMPayment.bill)
async def coupon_used(callback: types.CallbackQuery, state: FSMContext):
    args = callback.data.split('=')[1].split(',')

    if not User(callback.from_user.id).can_coupon_be_used(int(args[0]), int(args[1])):
        await callback.answer(f'Купон истёк или неактивен для данного курса', show_alert=True)
        return

    await callback.answer(convert_spec_text(f'С учётом купона цена составляет %spc%course_price_with_coupon={args[0]},{args[1]}%/spc%₽'), show_alert=True)
    async with state.proxy() as data:
        data['coupon'] = Coupon(int(args[1]), callback.from_user.id)


@dp.callback_query_handler(state=FSMPayment.course)
@dp.callback_query_handler(state=FSMPayment.bill)
async def payment_block(callback: types.CallbackQuery):
    await callback.answer('Необходимо отменить или завершить оплату', show_alert=True)


@dp.callback_query_handler(text_startswith="payment_start")
async def payment_start(callback: types.CallbackQuery, state: FSMContext):
    slide = Slide("payment_start")
    try:
        await slide.answer(callback)
        await state_modifier(slide.modifier)
    except Exception as err:
        await callback.answer("Что-то пошло не так")
        await state.finish()
        errorstack.add(f"ERR: payment_start {type(err).__name__} / {err}")
    await callback.answer()


@dp.callback_query_handler(text_startswith="paycheck=")
async def paycheck_slides(callback: types.CallbackQuery):
    modifiers = callback.data.split('=')[1].split(',')
    course_id = int(modifiers[0])
    accept_slide = int(modifiers[1])
    decline_slide = int(modifiers[2])

    if User(callback.from_user.id).is_course_paid(course_id):
        await Slide(accept_slide).answer(callback, is_bot_msg=True)
    else:
        await Slide(decline_slide).answer(callback, is_bot_msg=True)
    await callback.answer()


async def set_questionnaire_state(question: Question):
    if question.quest_type() == "multiple":
        await FSMQuest.quest_multiple.set()
    if question.quest_type() == 'buttons':
        await FSMQuest.quest_button.set()
    if question.quest_type() == 'text':
        await FSMQuest.quest_text.set()


@dp.callback_query_handler(text_startswith="questionnaire")
async def questionnaire_start(callback: types.CallbackQuery, state: FSMContext):
    try:
        question = Question(quest_id=int(callback.data.split('=')[1].split(',')[0]), user=User(callback.from_user.id))

        if question.is_ever_completed():
            await callback.answer("Опрос уже пройден")
            return

        await question.send()
        await set_questionnaire_state(question)
        async with state.proxy() as data:
            data['question'] = question

    except Exception as err:
        await callback.answer("Что-то пошло не так")
        errorstack.add(f"ERR: questionnaire_start {type(err).__name__} / {err}")
    await callback.answer()


async def questionnaire_results_send(question: Question):
    await Slide(message=f"Пользователь {question.user.name} {question.user.uname} {question.user.lastname} ответил на вопрос:\n{question.slide.message}\n\n{question.answer_as_text()}").send_to_group(moderator_ids)


async def questionnaire_start_blocker(message: types.Message, state: FSMContext):
    try:
        async with state.proxy() as data:
            question = data['question']
        await message.answer("Необходимо закончить предыдущий опрос")
        await question.send()
    except Exception as err:
        await message.answer("Что-то пошло не так")
        errorstack.add(f"ERR: questionnaire_start_blocker {type(err).__name__} / {err}")


async def questionnaire_answer(message: types.Message, state: FSMContext):
    try:
        async with state.proxy() as data:
            question = data['question']
        result = await question.give_answer(message.text)
        if result["success"] is False:
            await message.answer(result["err_message"])
            raise ValueError(result["err_message"])
        result = question.commit_answer()
        if result["success"] is False:
            await message.answer(result["err_message"])
            raise ValueError(result["err_message"])
        await question.send_next()
        await set_questionnaire_state(question)

    except Exception as err:
        errorstack.add(f"ERR: questionnaire_answer {type(err).__name__} / {err}")


@dp.callback_query_handler(text_startswith="answer=", state=FSMQuest.quest_button)
async def questionnaire_button_answer(callback: types.CallbackQuery, state: FSMContext):
    try:
        answer = callback.data.split(';')
        button_positions = answer[0].split('=')[1].split(',')
        button_row_num = int(button_positions[0])
        button_row_pos = int(button_positions[1])
        answer_slide_id = int(answer[1].split('=')[1])
        async with state.proxy() as data:
            question = data['question']
        result = await question.give_answer(Button(button_row_num, button_row_pos, answer_slide_id))
        if result["success"] is False:
            await callback.answer(result["err_message"])
            raise ValueError(result["err_message"])
        result = question.commit_answer()
        if result["success"] is False:
            await callback.answer(result["err_message"])
            raise ValueError(result["err_message"])
        await question.send_next()
        await set_questionnaire_state(question)

    except Exception as err:
        errorstack.add(f"ERR: questionnaire_button_answer {type(err).__name__} / {err}")


@dp.callback_query_handler(text_startswith="answer=", state=FSMQuest.quest_multiple)
async def questionnaire_multiple_answer(callback: types.CallbackQuery, state: FSMContext):
    try:
        answer = callback.data.split(';')
        button_positions = answer[0].split('=')[1].split(',')
        button_row_num = int(button_positions[0])
        button_row_pos = int(button_positions[1])
        answer_slide_id = int(answer[1].split('=')[1])
        async with state.proxy() as data:
            question = data['question']
        result = await question.give_answer(Button(button_row_num, button_row_pos, answer_slide_id))
        if result["success"] is False:
            await callback.answer(result["err_message"])
            raise ValueError(result["err_message"])

    except Exception as err:
        errorstack.add(f"ERR: questionnaire_multiple_answer {type(err).__name__} / {err}")


@dp.callback_query_handler(text_startswith="quest_commit", state=FSMQuest.quest_multiple)
async def questionnaire_multiple_apply(callback: types.CallbackQuery, state: FSMContext):
    try:
        async with state.proxy() as data:
            question = data['question']
        result = question.commit_answer()
        if result["success"] is False:
            await callback.answer(result["err_message"])
            raise ValueError(result["err_message"])
        await question.send_next()
        await set_questionnaire_state(question)

    except Exception as err:
        errorstack.add(f"ERR: questionnaire_multiple_apply {type(err).__name__} / {err}")


@dp.callback_query_handler(state=FSMQuest.quest_text)
@dp.callback_query_handler(state=FSMQuest.quest_button)
@dp.callback_query_handler(state=FSMQuest.quest_multiple)
async def questionnaire_block(callback: types.CallbackQuery):
    await callback.answer('Необходимо завершить опрос', show_alert=True)


@dp.callback_query_handler(text_startswith="slide_")
async def callback_slide(callback: types.CallbackQuery):
    try:
        slide_id = callback.data.split('_')[1]
        if slide_id != '':
            slide = Slide(slide_id)
            await slide.answer(callback)
            await state_modifier(slide.modifier)
        await callback.answer()
    except Exception as err:
        errorstack.add(f"ERR: callback_slide_handler {err}")
        await callback.answer("Что-то пошло не так")


def register_handlers_client():
    dp.register_message_handler(get_started, commands=['start'])
    dp.register_message_handler(payment_document_handler, state=FSMPayment.bill, content_types=['photo', 'document'])
    dp.register_message_handler(payment_non_document_handler, state=FSMPayment.bill, content_types=['video', 'voice', 'video_note', 'animation'])
    dp.register_message_handler(questionnaire_start_blocker, Text(startswith='/start', ignore_case=True), state=FSMQuest.quest_text)
    dp.register_message_handler(questionnaire_start_blocker, Text(startswith='/start', ignore_case=True), state=FSMQuest.quest_button)
    dp.register_message_handler(questionnaire_answer, state=FSMQuest.quest_text, content_types=types.ContentType.TEXT)
