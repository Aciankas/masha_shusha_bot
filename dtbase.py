import datetime
from create_bot import cur, base, errorstack


def current_timestamp():
    return f"to_timestamp('{datetime.datetime.now()}', 'YYYY-MM-DD HH24:MI:SS.US')"


def current_date():
    return f"to_date('{datetime.datetime.now()}', 'YYYY-MM-DD')"


def db_execute(command):
    try:
        cur.execute(command)
        if command.lower()[:6] == 'select':
            try:
                res = cur.fetchall()
            except IndexError:
                res = []
            return res
        else:
            base.commit()
    except Exception as err:
        cur.execute('rollback')
        print(f'ERRO: Commit {err}')


def spec_to_text(text, user_id=None):
    command = text
    try:
        text.replace(' ', '')
        spec_text_dict = {'merch_name': 'select name as result from md_merchandise where id = %arg%',
                          'merch_price': 'select price as result from md_merchandise where id = %arg%',
                          'merch_price_with_coupon': "select trunc( case when to_number(c.effect, '999999999.99') < 1 then "
                                                     "  sc.price*(1-to_number(c.effect, '999999999.99')) else"
                                                     "  sc.price-to_number(c.effect, '999999999.99') end ) as result"
                                                     "  from (select id, case when effect like '%\%%' escape '\\' then"
                                                     "    to_char(to_number(replace(effect, '%', ''), '99')/100, '99.99') else" 
                                                     "    effect end as effect "
                                                     "    from md_coupons) c"
                                                     "     , md_merchandise sc"
                                                     " where sc.id = %arg%"
                                                     "   and c.id = %arg%",
                          'coupon_expiration_time': f"select date_trunc('minutes', max(end_time + interval '3 hours')) as result "
                                                    f"  from ft_coupon_schedule "
                                                    f" where usr_id = {user_id} "
                                                    f"   and coupon_id = %arg% "
                                                    f"   and charges > 0"}
        command = spec_text_dict[text.split('=')[0]]
        args = text.split('=')[1].split(',')
        for arg in args:
            command = command.replace('%arg%', arg, 1)
        return db_execute(command)[0]["result"]
    except Exception as err:
        print('ERR spec_to_text: ' + str(err))
        return f"ERR:{command} " + str(err)


def set_md_bots_name(bot_id):
    try:
        db_execute(f"select * from md_bots where name = '{bot_id}'")
    except IndexError:
        db_execute(f"insert into md_bots (name) values ('{bot_id}')")


def set_start_slide(bot_id):
    try:
        db_execute(f"select * from md_slides where modifier = 'start' and bot_id = '{bot_id}'")
    except IndexError:
        db_execute(f"insert into md_slides (message, header, modifier, bot_id) values ('Стартовый слайд', '{bot_id}. Старт', 'start', '{bot_id}')")


def get_slide_deprecated(slide_id, bot_id):  # DEPRECATED
    try:
        if type(slide_id) is int or slide_id.isdigit():
            return db_execute(f"select s.id, m.media_id, s.message, m.type, "
                              f"s.bot_id, s.modifier, s.appearance_mod, s.schedule_set, s.schedule_priority, s.header "
                              f"from md_slides s "
                              f"left join md_media m on s.media_id = m.id "
                              f"where s.id = {slide_id} "
                              f"and s.bot_id = '{bot_id}'")[0]
        else:
            return db_execute(f"select s.id, m.media_id, s.message, m.type, "
                              f"s.bot_id, s.modifier, s.appearance_mod, s.schedule_set, s.schedule_priority, s.header "
                              f"from md_slides s "
                              f"left join md_media m on s.media_id = m.id "
                              f"where s.modifier = '{slide_id}' "
                              f"and s.bot_id = '{bot_id}'")[0]
    except IndexError:
        errorstack.add(f"get_slide({slide_id}, '{bot_id}') not found")


def get_slide(slide_id, bot_id):
    try:
        if type(slide_id) is int or slide_id.isdigit():
            search = f"s.id = {slide_id}"
        else:
            search = f"s.modifier = '{slide_id}'"
        return db_execute(f"select * from md_slides s "
                          f"where {search} "
                          f"and s.bot_id = '{bot_id}'")[0]
    except Exception as err:
        errorstack.add(f"get_slide({slide_id}, '{bot_id}') {err}")


def get_start_arg(argument, bot_id):
    return db_execute(f"select s.slide_id from st_start_args s, md_slides sl "
                      f"where s.slide_id = sl.id and arg = '{argument}' and sl.bot_id = '{bot_id}'")[0]["slide_id"]


def get_userdata_by_id(user_id):
    return db_execute(f"select * from md_user_data u where id = {user_id}")[0]


def insert_media(media_id: str, media_type: str):
    return db_execute(f"insert into md_media (type, media_id) values ('{media_type}', '{media_id}')")


def get_media_by_id(db_id):
    return db_execute(f"select * from md_media where id = {db_id}")[0]


def get_mediagroup(group_id):
    return db_execute(f"select m.media_id, m.type, g.order_id from md_mediagroups g, md_media m where m.id = g.media_id and g.group_id = {group_id} order by g.order_id")


def get_medialist_first(group_id):
    return get_mediagroup(group_id)[0]


def get_medialist_cnt(group_id, media_cnt):
    # return db_execute(f"select type, media_id from media "
    #                   f"where id = (select media_id_{media_cnt} from mediagroups where id = {medialist_id})")[0]
    return get_medialist_first(group_id)  # ПОЛНОСТЬЮ ПЕРЕРАБОТАТЬ, ЭТО ГОВНО


def get_keyboard(slide_id):
    return db_execute(f"select * from md_buttons where slide_id = {slide_id} order by row_num, row_pos")


def get_course_by_id(course_id):
    return db_execute(f"select * from md_courses where id = {course_id}")[0]


def get_merch_by_id(merch_id):
    return db_execute(f"select * from md_merchandise where id = {merch_id}")[0]


def get_courses_by_merch_id(merch_id):
    return db_execute(f"select c.* from md_courses c, ln_course_merch l where l.course_id = c.id and l.merch_id = {merch_id}")


def reg_user(usr_id, name, uname, lastname):
    db_execute(f"merge into md_user_data u "
               f"using (select {usr_id} as id, '{name}' as name, '{uname}' as uname, '{lastname}' as lastname, {current_date()} as reg_date) as mg "
               f"on mg.id = u.id "
               f"when not matched then "
               f"insert values(mg.id, mg.name, mg.uname, mg.lastname, mg.reg_date) "
               f"when matched then "
               f"update set name = mg.name, uname = mg.uname, lastname = mg.lastname")


def click_log(usr_id, slide_id, bot_msg=False):
    db_execute(f"merge into ft_user_activity u "
               f"using(select {usr_id} as usr_id, {slide_id} as slide_id, "
               f"1 as counter, {current_timestamp()} as last_time, {bot_msg} as bot_msg) as mg "
               f"on mg.usr_id = u.usr_id and mg.slide_id = u.slide_id and mg.bot_msg = u.bot_msg when not matched then "
               f"insert values(mg.usr_id, mg.slide_id, mg.counter, mg.last_time, mg.bot_msg) "
               f"when matched then "
               f"update set counter = u.counter + 1, last_time = mg.last_time")


def get_transactions(usr_id, merch_id, *status_list):  # 'processing', 'reject', 'commit'
    status_string = "'" + "', '".join(status_list) + "'"
    return db_execute(f"select t.id, u.id as user_id, u.uname as username, "
                      f"c.id as merch_id, c.name as merch_name, t.media_id, t.type, t.status, t.coupon_id "
                      f"from ft_transactions t"
                      f", md_user_data u"
                      f", md_merchandise c "
                      f"where t.usr_id = u.id "
                      f"and t.merch_id = c.id "
                      f"and u.id = {usr_id} "
                      f"and c.id = {merch_id} "
                      f"and t.status in ({status_string})"
                      f"order by t.id desc")


def get_transaction_by_id(t_id):
    try:
        return db_execute(f"select t.id, u.id as user_id, u.uname as username, "
                          f"c.id as merch_id, c.name as merch_name, t.media_id, t.type, t.status, t.coupon_id "
                          f"from ft_transactions t"
                          f", md_user_data u "
                          f", md_merchandise c "
                          f"where t.usr_id = u.id "
                          f"and t.merch_id = c.id "
                          f"and t.id = {t_id} ")[0]
    except IndexError:
        print(f"get_transaction_by_id({t_id}) not found")


def update_transaction_status(t_id, status):
    db_execute(f"update ft_transactions set status = '{status}' where id = {t_id}")


def create_transaction(user_id, merch_id, media_id, media_type, coupon_id=None):
    if coupon_id is None:
        coupon_id = 'null'
    db_execute(f"insert into ft_transactions (usr_id, merch_id, media_id, type, coupon_id) "
               f"values ({user_id}, {merch_id}, '{media_id}', '{media_type}', {coupon_id})")
    if coupon_id != 'null':
        use_coupon(user_id, coupon_id)


def get_slides_from_list_visited(slides: list, user_id: int):
    list_string = ",".join(slides)
    try:
        return int(db_execute(f"select count(*) from ft_user_activity where slide_id in ({list_string}) and usr_id = {user_id}")[0]["count"])
    except IndexError:
        print(f"user_active_slides({list_string}, {user_id}) not found")


def get_questionnaire_start_slide(quest_id: int):
    try:
        return int(db_execute(f"select * from md_questionnaire where quest_id = {quest_id} and slide_id not in (select next_id from md_questionnaire)")[0]["slide_id"])
    except IndexError:
        print(f"get_questionnaire_start_slide({quest_id}) not found")


def get_questionnaire_finish_slide(quest_id: int):
    try:
        return int(db_execute(f"select * from md_questionnaire where quest_id = {quest_id} and next_id not in (select slide_id from md_questionnaire)")[0]["next_id"])
    except IndexError:
        print(f"get_questionnaire_start_slide({quest_id}) not found")


def get_questionnaire_next_slide(slide_id):
    try:
        return db_execute(f"select '{slide_id}' as slide_prev, q1.next_id as slide_id, q2.next_id from md_questionnaire q1 left join md_questionnaire q2 on q2.slide_id = q1.next_id where q1.slide_id = {slide_id}")[0]
    except IndexError:
        print(f"get_questionnaire_next_slide({slide_id}) not found")


def set_questionnaire_answer(slide_id, user_id, message):
    db_execute(f"insert into ft_answers (slide_id, usr_id, message) values ({slide_id}, {user_id},'{message}')")


def get_scheduled_be_send(bot_id):
    return db_execute(f"select sh.*, sl.bot_id from ft_schedule sh, md_slides sl where sh.slide_id = sl.id and sl.bot_id = '{bot_id}' and send_time < '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' order by sl.schedule_priority")


def delete_scheduled(send_time, usr_id, slide_id):
    db_execute(f"delete from ft_schedule where send_time = '{str(send_time)}' and usr_id = {usr_id} and slide_id = {slide_id};")


def create_scheduled(send_time, usr_id, slide_id, modifier=None):
    db_execute(f"insert into ft_schedule (send_time, usr_id, slide_id, modifier) values ('{str(send_time)}', {usr_id}, {slide_id}, '{modifier}')")


def is_scheduled_exists(send_time, usr_id, slide_id):
    return db_execute(f"select * from ft_schedule where send_time = '{str(send_time)}' and usr_id = {usr_id} and slide_id = {slide_id}")


def delete_for_blocked(usr_id, bot_id):
    db_execute(f"delete from ft_user_activity where usr_id = {usr_id} and slide_id in (select id from md_slides where del_if_blocked = True and bot_id = '{bot_id}')")
    db_execute(f"delete from ft_schedule where usr_id = {usr_id} and slide_id in (select id from md_slides where bot_id = '{bot_id}')")


def help_cmd_select(usr_id):
    return db_execute(f"select c.name, c.description, c.command, c.upload_type from md_commands c where c.rights in (select rights from md_access where usr_id = {usr_id})")


def get_coupon_by_id(identifier):
    return db_execute(f"select * from md_coupons where id = {identifier}")[0]


def create_scheduled_coupon(end_time, usr_id, coupon_id, charges):
    db_execute(f"insert into ft_coupon_schedule (end_time, usr_id, coupon_id, charges) values ('{str(end_time)}', {usr_id}, {coupon_id}, {charges})")


def get_scheduled_coupon(coupon_id: int, user_id: int):
    return db_execute(f"select c.name, c.effect, c.end_slide_id, s.end_time, s.charges from ft_coupon_schedule s, md_coupons c where c.id = s.coupon_id and usr_id = {user_id} and coupon_id = {coupon_id}")[0]


def get_coupon_merch_ids(coupon_id: int):
    return db_execute(f"select id_merch from ln_coupon_merch where coupon_id = {coupon_id}")


def get_coupons_for_merch_and_user(usr_id: int, merch_id: int):
    return db_execute(f"select c.id, c.name, c.effect, c.end_slide_id, s.end_time, s.charges from ft_coupon_schedule s, md_coupons c, ln_coupon_merch l where c.id = s.coupon_id and l.coupon_id = s.coupon_id and usr_id = {usr_id} and merch_id = {merch_id}")


def get_scheduled_coupons_be_closed(bot_id):
    return db_execute(f"select s.*, c.*, sl.bot_id from ft_coupon_schedule s, md_coupons c, md_slides sl "
                      f"where c.id = s.coupon_id and c.end_slide_id = sl.id and sl.bot_id = '{bot_id}' "
                      f"and end_time < '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'")


def cancel_coupon(end_time, usr_id, coupon_id):
    db_execute(f"delete from ft_coupon_schedule where end_time = '{str(end_time)}' and usr_id = {usr_id} and coupon_id = {coupon_id}")


def get_coupons_for_merch(usr_id, merch_id):
    return db_execute(f"select c.id, c.name, c.effect, s.charges from ft_coupon_schedule s, md_coupons c, ln_coupon_merch l where l.coupon_id = s.coupon_id and s.coupon_id = c.id and l.merch_id = {merch_id} and s.usr_id = {usr_id} order by s.end_time")


def can_coupon_be_used(usr_id, merch_id, coupon_id):
    return db_execute(f"select count(*) from ft_coupon_schedule s, ln_coupon_merch l where s.coupon_id = l.coupon_id and s.charges > 0 and s.usr_id = {usr_id} and s.coupon_id = {coupon_id} and l.merch_id = {merch_id}")[0]['count'] >= 1


def is_coupon_active(usr_id, coupon_id):
    return db_execute(f"select count(*) from ft_coupon_schedule s where s.usr_id = {usr_id} and s.charges > 0 and coupon_id = {coupon_id}")[0]["count"] >= 1


def is_course_paid(usr_id, course_id):
    return db_execute(f"select count(*) from ft_transactions t, ln_course_merch l where t.usr_id = {usr_id} and t.status = 'commit' and t.merch_id = l.merch_id and l.course_id = {course_id}")[0]["count"] >= 1


def use_coupon(usr_id, coupon_id):
    if coupon_id is not None and usr_id is not None:
        db_execute(f"update ft_coupon_schedule s set charges = charges - 1 where s.usr_id = {usr_id} and coupon_id = {coupon_id}")


def return_coupon(usr_id, coupon_id):
    if coupon_id is not None and usr_id is not None:
        db_execute(f"update ft_coupon_schedule s set charges = charges + 1 where s.usr_id = {usr_id} and coupon_id = {coupon_id}")


def create_new_button(slide_id):
    db_execute(f"insert into md_buttons (row_num, row_pos, slide_id, name) "
               f"values (coalesce((select max(row_num)+1 from buttons where slide_id = {slide_id}), 1), 1, {slide_id}, 'New button')")


def get_button_by_ids(slide_id, row_num, row_pos):
    return db_execute(f"select * from md_buttons where slide_id = {slide_id} and row_num = {row_num} and row_pos = {row_pos}")[0]


def delete_button_by_ids(slide_id, row_num, row_pos):
    db_execute(f"delete from md_buttons "
               f"where slide_id = {slide_id} and row_num = {row_num} and row_pos = {row_pos}")
