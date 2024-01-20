from aiogram import types
from create_bot import dp, delete_message, admin_ids, group_msg
from dtbase import db_execute, current_timestamp, help_cmd_select
from handlers.admin import commit_db_command


async def command_message_handler(message: types.Message):
    try:
        cmd_list = help_cmd_select(message.from_user.id)
        if cmd_list:
            if message.text == '/help':
                text = ''
                for cmd in cmd_list:
                    text += f"\n{cmd['name']} - {cmd['description']}"
                await group_msg(text, [message.from_user.id])
            else:
                found = False
                for cmd in cmd_list:
                    if cmd['name'] == message.text:
                        found = True
                        message.text = cmd['command']
                        await commit_db_command(message, cmd["upload_type"])
                        break
                if not found:
                    await delete_message(message)
        else:
            await delete_message(message)
    except Exception as err:
        await group_msg(f"command message error: {err}.\nmessage: {message}")


async def text_message_handler(message: types.Message):
    print(str((message.from_user.full_name or 'null')) + '; ' +
          str((message.from_user.first_name or 'null')) + '; ' +
          str((message.from_user.last_name or 'null')) + '; ' +
          str((message.from_user.language_code or 'null')) + '; ' +
          str((message.from_user.locale or 'null')) + '; ' +
          str((message.from_user.username or 'null')) + '; ' +
          str((message.from_user.url or 'null')) + '; ' +
          ' (' + str(message.from_user.id) + '): ' + message.text)
    await delete_message(message)


async def photo_message_handler(message: types.Message):
    if message.from_user.id in admin_ids:
        await group_msg(message.photo[0].file_id, None, 'photo', message.photo[0].file_id)
    db_execute(f"insert into media_dump (usr_id, type, id, message, time) values ({message.from_user.id}, 'photo', '{message.photo[0].file_id}', '{(message.caption or '')}', {current_timestamp()})")
    await delete_message(message)


async def video_message_handler(message: types.Message):
    if message.from_user.id in admin_ids:
        await group_msg(message.video.file_id, None, 'video', message.video.file_id)
    db_execute(f"insert into media_dump (usr_id, type, id, message, time) values ({message.from_user.id}, 'video', '{message.video.file_id}', '{(message.caption or '')}', {current_timestamp()})")
    await delete_message(message)


async def document_message_handler(message: types.Message):
    if message.from_user.id in admin_ids:
        await group_msg(message.document.file_id, None, 'document', message.document.file_id)
    db_execute(f"insert into media_dump (usr_id, type, id, message, time) values ({message.from_user.id}, 'document', '{message.document.file_id}', '{(message.caption or '')}', {current_timestamp()})")
    await delete_message(message)


async def voice_message_handler(message: types.Message):
    if message.from_user.id in admin_ids:
        await group_msg(message.voice.file_id, None, 'voice', message.voice.file_id)
    db_execute(f"insert into media_dump (usr_id, type, id, message, time) values ({message.from_user.id}, 'voice', '{message.voice.file_id}', '{(message.caption or '')}', {current_timestamp()})")
    await delete_message(message)


async def video_note_message_handler(message: types.Message):
    if message.from_user.id in admin_ids:
        await group_msg(message.video_note.file_id, None, 'video_note', message.video_note.file_id)
    db_execute(f"insert into media_dump (usr_id, type, id, message, time) values ({message.from_user.id}, 'video_note', '{message.video_note.file_id}', '{(message.caption or '')}', {current_timestamp()})")
    await delete_message(message)


async def gif_message_handler(message: types.Message):
    if message.from_user.id in admin_ids:
        await group_msg(message.animation.file_id, None, 'animation', message.animation.file_id)
    db_execute(f"insert into media_dump (usr_id, type, id, message, time) values ({message.from_user.id}, 'gif', '{message.animation.file_id}', '{(message.caption or '')}', {current_timestamp()})")
    await delete_message(message)


@dp.callback_query_handler()
async def callback_anything_but_slide(callback: types.CallbackQuery):
    await callback.answer()


def register_handlers_other():
    dp.register_message_handler(command_message_handler, lambda message: message.text[0] == '/', state="*")
    dp.register_message_handler(text_message_handler, state="*")
    dp.register_message_handler(photo_message_handler, content_types=['photo'], state="*")
    dp.register_message_handler(video_message_handler, content_types=['video'], state="*")
    dp.register_message_handler(document_message_handler, content_types=['document'], state="*")
    dp.register_message_handler(voice_message_handler, content_types=['voice'], state="*")
    dp.register_message_handler(video_note_message_handler, content_types=['video_note'], state="*")
    dp.register_message_handler(gif_message_handler, content_types=['animation'], state="*")
