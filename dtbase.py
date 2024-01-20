import datetime
import psycopg2 as ps
import psycopg2.extras
import os


def current_timestamp():
    return f"to_timestamp('{datetime.datetime.now()}', 'YYYY-MM-DD HH24:MI:SS.US')"


def current_date():
    return f"to_date('{datetime.datetime.now()}', 'YYYY-MM-DD')"


try:
    base = ps.connect(os.environ.get('DATABASE_URL'), sslmode='require')
    cur = base.cursor(cursor_factory=ps.extras.RealDictCursor)
    print(f'База подключена, URL: {os.environ.get("DATABASE_URL")}')
except Exception as error:
    base = None
    cur = None
    print(f'!!База не подключена!! {error}')


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
        spec_text_dict = {'course_name': 'select name as result from st_courses where id = %arg%',
                          'course_price': 'select price as result from st_courses where id = %arg%',
                          'course_price_with_coupon': "select trunc( case when to_number(c.effect, '999999999.99') < 1 then "
                                                      "  sc.price*(1-to_number(c.effect, '999999999.99')) else"
                                                      "  sc.price-to_number(c.effect, '999999999.99') end ) as result"
                                                      "  from (select id, case when effect like '%\%%' escape '\\' then"
                                                      "    to_char(to_number(replace(effect, '%', ''), '99')/100, '99.99') else" 
                                                      "    effect end as effect "
                                                      "    from coupons) c"
                                                      "     , st_courses sc"
                                                      " where sc.id = %arg%"
                                                      "   and c.id = %arg%",
                          'coupon_expiration_time': f"select date_trunc('minutes', max(end_time + interval '3 hours')) as result "
                                                    f"  from coupon_schedule "
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


def set_st_bots_name(bot_id):
    try:
        db_execute(f"select * from st_bots where name = '{bot_id}'")
    except IndexError:
        db_execute(f"insert into st_bots (name) values ('{bot_id}')")


def set_start_slide(bot_id):
    try:
        db_execute(f"select * from slides where modifier = 'start' and bot_id = '{bot_id}'")
    except IndexError:
        db_execute(f"insert into slides (message, header, modifier, bot_id) values ('Стартовый слайд', '{bot_id}. Старт', 'start', '{bot_id}')")


def get_slide(slide_id, bot_id):
    try:
        if type(slide_id) is int or slide_id.isdigit():
            return db_execute(f"select s.id, m.media_id, s.message, m.type, "
                              f"s.bot_id, s.modifier, s.appearance_mod, s.schedule_set, s.schedule_priority, s.header "
                              f"from slides s "
                              f"left join media m on s.media_id = m.id "
                              f"where s.id = {slide_id} "
                              f"and s.bot_id = '{bot_id}'")[0]
        else:
            return db_execute(f"select s.id, m.media_id, s.message, m.type, "
                              f"s.bot_id, s.modifier, s.appearance_mod, s.schedule_set, s.schedule_priority, s.header "
                              f"from slides s "
                              f"left join media m on s.media_id = m.id "
                              f"where s.modifier = '{slide_id}' "
                              f"and s.bot_id = '{bot_id}'")[0]
    except IndexError:
        print(f"get_slide({slide_id}, '{bot_id}') not found")


def get_start_arg(argument, bot_id):
    return db_execute(f"select s.slide_id from start_args s, slides sl "
                      f"where s.slide_id = sl.id and arg = '{argument}' and sl.bot_id = '{bot_id}'")[0]["slide_id"]


def get_userdata_by_id(user_id):
    return db_execute(f"select * from user_data u where id = {user_id}")[0]


def get_mediagroup(group_num):
    return db_execute(f"select m1.media_id || ' ' || m1.type as media_id_1, "
                      f"m2.media_id || ' ' || m2.type as media_id_2, "
                      f"m3.media_id || ' ' || m3.type as media_id_3, "
                      f"m4.media_id || ' ' || m4.type as media_id_4, "
                      f"m5.media_id || ' ' || m5.type as media_id_5, "
                      f"m6.media_id || ' ' || m6.type as media_id_6, "
                      f"m7.media_id || ' ' || m7.type as media_id_7, "
                      f"m8.media_id || ' ' || m8.type as media_id_8, "
                      f"m9.media_id || ' ' || m9.type as media_id_9, "
                      f"m10.media_id || ' ' || m10.type as media_id_10 "
                      f'from mediagroups gr '
                      f'left join media m1 on gr.media_id_1 = m1.id '
                      f'left join media m2 on gr.media_id_2 = m2.id '
                      f'left join media m3 on gr.media_id_3 = m3.id '
                      f'left join media m4 on gr.media_id_4 = m4.id '
                      f'left join media m5 on gr.media_id_5 = m5.id '
                      f'left join media m6 on gr.media_id_6 = m6.id '
                      f'left join media m7 on gr.media_id_7 = m7.id '
                      f'left join media m8 on gr.media_id_8 = m8.id '
                      f'left join media m9 on gr.media_id_9 = m9.id '
                      f'left join media m10 on gr.media_id_10 = m10.id '
                      f'where gr.id = {group_num}')[0]


def get_medialist_first(medialist_id):
    return db_execute(f"select mr.type, mr.media_id "
                      f"from media mr, mediagroups mg "
                      f"where mr.id = mg.media_id_1 "
                      f"and mg.id = {medialist_id}")[0]


def get_medialist_cnt(medialist_id, media_cnt):
    return db_execute(f"select type, media_id from media "
                      f"where id = (select media_id_{media_cnt} from mediagroups where id = {medialist_id})")[0]


def get_keyboard(slide_id):
    return db_execute(f"select * from buttons where slide_id = {slide_id} order by row_num, row_pos")


def get_course_by_id(course_id):
    try:
        return db_execute(f"select * from st_courses where id = {course_id}")[0]
    except IndexError:
        print(f"get_course_by_id({course_id}) not found")


def reg_user(usr_id, name, uname, lastname):
    db_execute(f"merge into user_data u "
               f"using (select {usr_id} as id, '{name}' as name, '{uname}' as uname, '{lastname}' as lastname, {current_date()} as reg_date) as mg "
               f"on mg.id = u.id "
               f"when not matched then "
               f"insert values(mg.id, mg.name, mg.uname, mg.lastname, mg.reg_date) "
               f"when matched then "
               f"update set name = mg.name, uname = mg.uname, lastname = mg.lastname")


def click_log(usr_id, slide_id, bot_msg=False):
    db_execute(f"merge into user_activity u "
               f"using(select {usr_id} as usr_id, {slide_id} as slide_id, "
               f"1 as counter, {current_timestamp()} as last_time, {bot_msg} as bot_msg) as mg "
               f"on mg.usr_id = u.usr_id and mg.slide_id = u.slide_id and mg.bot_msg = u.bot_msg when not matched then "
               f"insert values(mg.usr_id, mg.slide_id, mg.counter, mg.last_time, mg.bot_msg) "
               f"when matched then "
               f"update set counter = u.counter + 1, last_time = mg.last_time")


def get_transactions(usr_id, course_id, *status_list):  # 'processing', 'reject', 'commit'
    status_string = "'" + "', '".join(status_list) + "'"
    return db_execute(f"select t.id, u.id as user_id, u.uname as username, "
                      f"c.id as course_id, c.name as course_name, t.media_id, t.type "
                      f"from transactions t"
                      f", user_data u"
                      f", st_courses c "
                      f"where t.usr_id = u.id "
                      f"and t.course_id = c.id "
                      f"and u.id = {usr_id} "
                      f"and c.id = {course_id} "
                      f"and t.status in ({status_string})")


def get_transaction_by_id(t_id):
    try:
        return db_execute(f"select t.id, u.id as user_id, u.uname as username, "
                          f"c.id as course_id, c.name as course_name, t.media_id, t.type, t.status, t.coupon_id "
                          f"from transactions t"
                          f", user_data u"
                          f", st_courses c "
                          f"where t.usr_id = u.id "
                          f"and t.course_id = c.id "
                          f"and t.id = {t_id} ")[0]
    except IndexError:
        print(f"get_transaction_by_id({t_id}) not found")


def update_transaction_status(t_id, status):
    db_execute(f"update transactions set status = '{status}' where id = {t_id}")


def get_bot_link_by_course(course_id):
    try:
        return db_execute(f"select bot_link from st_courses c, st_bots b where c.bot_id = b.name and c.id = {course_id}")[0]
    except IndexError:
        print(f"get_bot_link_by_course({course_id}) not found")


def transaction_create(user_id, course_id, media_id, media_type, coupon_id=None):
    if coupon_id is None:
        coupon_id = 'null'
    db_execute(f"insert into transactions (usr_id, course_id, media_id, type, coupon_id) "
               f"values ({user_id}, {course_id}, '{media_id}', '{media_type}', {coupon_id})")
    if coupon_id != 'null':
        coupon_use(user_id, coupon_id)


def user_active_slides(slide_list, user_id):
    list_string = ",".join(slide_list)
    try:
        return int(db_execute(f"select count(*) from user_activity where slide_id in ({list_string}) and usr_id = {user_id}")[0]["count"])
    except IndexError:
        print(f"user_active_slides({list_string}, {user_id}) not found")


def get_questionnaire_start_slide(quest_id):
    try:
        return int(db_execute(f"select * from questionnaire where modifier = '{quest_id}.start'")[0]["slide_id"])
    except IndexError:
        print(f"get_questionnaire_start_slide({quest_id}) not found")


def get_questionnaire_next_slide(slide_id):
    try:
        return db_execute(f"select '{slide_id}' as slide_prev, q1.next_id as slide_id, q2.next_id, q1.modifier from questionnaire q1 left join questionnaire q2 on q2.slide_id = q1.next_id where q1.slide_id = {slide_id}")[0]
    except IndexError:
        print(f"get_questionnaire_next_slide({slide_id}) not found")


def set_questionnaire_answer(slide_id, user_id, message):
    db_execute(f"insert into answers (slide_id, usr_id, message) values ({slide_id}, {user_id},'{message}')")


def scheduled_be_send(bot_id):
    return db_execute(f"select sh.*, sl.bot_id from schedule sh, slides sl where sh.slide_id = sl.id and sl.bot_id = '{bot_id}' and send_time < '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}' order by sl.schedule_priority")


def delete_scheduled(send_time, usr_id, slide_id):
    db_execute(f"delete from schedule where send_time = '{str(send_time)}' and usr_id = {usr_id} and slide_id = {slide_id};")


def create_scheduled(send_time, usr_id, slide_id, modifier=None):
    db_execute(f"insert into schedule (send_time, usr_id, slide_id, modifier) values ('{str(send_time)}', {usr_id}, {slide_id}, '{modifier}')")


def scheduled_exists(send_time, usr_id, slide_id):
    return db_execute(f"select * from schedule where send_time = '{str(send_time)}' and usr_id = {usr_id} and slide_id = {slide_id}")


def delete_for_blocked(usr_id, bot_id):
    db_execute(f"delete from user_activity where usr_id = {usr_id} and slide_id in (select id from slides where del_if_blocked = True and bot_id = '{bot_id}')")
    db_execute(f"delete from schedule where usr_id = {usr_id} and slide_id in (select id from slides where bot_id = '{bot_id}')")


def help_cmd_select(usr_id):
    return db_execute(f"select c.name, c.description, c.command, c.upload_type from commands c where c.rights in (select rights from access where usr_id = {usr_id})")


def get_coupon_by_id(identifier):
    return db_execute(f"select * from coupons where id = {identifier}")[0]


def create_scheduled_coupon(end_time, usr_id, coupon_id, charges):
    db_execute(f"insert into coupon_schedule (end_time, usr_id, coupon_id, charges) values ('{str(end_time)}', {usr_id}, {coupon_id}, {charges})")


def scheduled_coupons_be_closed(bot_id):
    return db_execute(f"select s.*, c.*, sl.bot_id from coupon_schedule s, coupons c, slides sl "
                      f"where c.id = s.coupon_id and c.end_slide_id = sl.id and sl.bot_id = '{bot_id}' "
                      f"and end_time < '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'")


def cancel_coupon(end_time, usr_id, coupon_id):
    db_execute(f"delete from coupon_schedule where end_time = '{str(end_time)}' and usr_id = {usr_id} and coupon_id = {coupon_id}")


def coupons_for_course(usr_id, course_id):
    return db_execute(f"select c.id, c.name, c.effect, s.charges from coupon_schedule s, coupons c where c.id = s.coupon_id and s.usr_id = {usr_id} and s.charges > 0 and regexp_like(course_list, '[[:<:]]{course_id}[[:>:]]') order by end_time")


def can_coupon_be_used(usr_id, course_id, coupon_id):
    return db_execute(f"select count(*) "
                      f"  from coupon_schedule s, coupons c "
                      f" where c.id = s.coupon_id "
                      f"   and s.usr_id = {usr_id}" 
                      f"   and s.charges > 0 "
                      f"   and regexp_like(course_list, '[[:<:]]{course_id}[[:>:]]')"
                      f"   and c.id = {coupon_id}")[0]['count'] == 1


def is_coupon_active(usr_id, coupon_id):
    return db_execute(f"select count(*) from coupon_schedule s where s.usr_id = {usr_id} and s.charges > 0 and coupon_id = {coupon_id}")[0]["count"] >= 1


def is_course_paid(usr_id, course_id):
    return db_execute(f"select count(*) from transactions t where t.usr_id = {usr_id} and t.status = 'commit' and t.course_id = {course_id}")[0]["count"] >= 1


def coupon_use(usr_id, coupon_id):
    if coupon_id is not None and usr_id is not None:
        db_execute(f"update coupon_schedule s set charges = charges - 1 where s.usr_id = {usr_id} and coupon_id = {coupon_id}")


def coupon_return(usr_id, coupon_id):
    if coupon_id is not None and usr_id is not None:
        db_execute(f"update coupon_schedule s set charges = charges + 1 where s.usr_id = {usr_id} and coupon_id = {coupon_id}")


def create_new_button(slide_id):
    db_execute(f"insert into buttons (row_num, row_pos, slide_id, name) "
               f"values (coalesce((select max(row_num)+1 from buttons where slide_id = {slide_id}), 1), 1, {slide_id}, 'New button')")


def get_button_by_ids(slide_id, row_num, row_pos):
    return db_execute(f"select * from buttons where slide_id = {slide_id} and row_num = {row_num} and row_pos = {row_pos}")[0]


def delete_button_by_ids(slide_id, row_num, row_pos):
    db_execute(f"delete from buttons "
               f"where slide_id = {slide_id} and row_num = {row_num} and row_pos = {row_pos}")


def slide_has_buttons(slide_id):
    return db_execute(f"select count(*) from slides s, buttons b where s.id = b.slide_id and s.id = {slide_id}")[0]["count"] > 0


def questionnaire_multiple_commit(slide_id, user_id, confirmed_answers):
    cmd = 'insert into answers (slide_id, usr_id, message) values '
    for answer in confirmed_answers:
        cmd += f"({slide_id}, {user_id}, (select name from buttons where slide_id = {slide_id} and row_num = {answer['row_num']} and row_pos = {answer['row_pos']})),"
    cmd = cmd[0:-1]
    db_execute(cmd)
