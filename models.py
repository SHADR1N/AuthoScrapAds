import datetime
from typing import *
import aiomysql

from common.config import config
from common.settings import CONNECT_TO_REMOTELY_DB
from common.db.mysql import BaseMySql


class DBServices(BaseMySql):
    """"""
    def __init__(self, *args, **kwargs):
        super(DBServices, self).__init__(*args, **kwargs)

    async def create_db_and_tables(self):
        await self.create_db_if_not_exist()

        pool = await aiomysql.create_pool(**self.pool_params)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SHOW TABLES;")
                existing_tables = await cursor.fetchall()
                existing_tables = [d[0] for d in existing_tables]

                if 'cars' not in existing_tables:
                    await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS cars (
                    id                          INTEGER(12)     UNSIGNED    NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    ad_id                       VARCHAR(255)    NOT NULL ,
                    Car_name                    VARCHAR(255)    NOT NULL DEFAULT '',
                    Phone                       TEXT            NOT NULL DEFAULT '',
                    Price_rub                   VARCHAR(255)    NOT NULL DEFAULT '',
                    Price_dollar                VARCHAR(255)    NOT NULL DEFAULT '',
                    Year                        VARCHAR(255)    NOT NULL DEFAULT '',
                    Vin                         VARCHAR(255)    NOT NULL DEFAULT '',
                    Сar_mileage                 VARCHAR(255)    NOT NULL DEFAULT '',
                    Fuel_type                   VARCHAR(255)    NOT NULL DEFAULT '',
                    Volume                      VARCHAR(255)    NOT NULL DEFAULT '',
                    Colour                      VARCHAR(255)    NOT NULL DEFAULT '',
                    Car_Type                    VARCHAR(255)    NOT NULL DEFAULT '',
                    Transmission                VARCHAR(255)    NOT NULL DEFAULT '',
                    Drive_Unit                  VARCHAR(255)    NOT NULL DEFAULT '',
                    Photos                      TEXT            NOT NULL DEFAULT '',
                    Site_link                   VARCHAR(255)    NOT NULL DEFAULT '',
                    Site_number                 VARCHAR(255)    NOT NULL DEFAULT '',
                    Publish_date                VARCHAR(255)    NOT NULL DEFAULT '',
                    Owner_name                  VARCHAR(255)    NOT NULL DEFAULT '',
                    Owner_city                  VARCHAR(255)    NOT NULL DEFAULT '',
                    Description                 TEXT            NOT NULL DEFAULT '',
                    Auto_info                   TEXT            NOT NULL DEFAULT '',
                    Brand                       VARCHAR(255)    NOT NULL DEFAULT '',
                    Model                       VARCHAR(255)    NOT NULL DEFAULT '',
                    Vin_Checked                 BOOLEAN         NOT NULL DEFAULT FALSE,
                    Condition_car               VARCHAR(255)    NOT NULL DEFAULT '',
                    Number_of_seats             VARCHAR(255)    NOT NULL DEFAULT '',
                    ShortLocation               VARCHAR(255)    NOT NULL DEFAULT '',
                    Generation                  VARCHAR(255)    NOT NULL DEFAULT '',
                    Generation_with_years       VARCHAR(255)    NOT NULL DEFAULT '',
                    Refreshed_at                VARCHAR(255)    NOT NULL DEFAULT '',
                    num_changes                 INTEGER(10)     NOT NULL DEFAULT 0,
                    updated_at                  DATETIME        NULL,
                    parsed_at                   DATETIME        NOT NULL DEFAULT NOW(),
                    is_active                   TINYINT(1)      NOT NULL DEFAULT 1,
                    was_sent                    TINYINT(1)      NOT NULL DEFAULT 0,
                    last_sent_data              TEXT            NOT NULL DEFAULT '',
                    last_sent_at                DATETIME        NULL,
                    ad_was_raised               DATETIME        NULL,
                    date_of_check               DATETIME        NULL ,
                    date_sending_check          DATETIME        NULL ,
                    UNIQUE KEY (ad_id(100))
                    )""")
                await conn.commit()

                if 'equipment' not in existing_tables:
                    await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS equipment (
                    id                          INTEGER(12)     UNSIGNED    NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    group_name                  VARCHAR(100)    NOT NULL DEFAULT '',
                    en_name                     VARCHAR(100)    NOT NULL DEFAULT '',
                    ru_name                     VARCHAR(100)    NOT NULL DEFAULT '',
                    UNIQUE KEY (en_name, ru_name)
                    )""")
                await conn.commit()

                if 'work_statistics' not in existing_tables:
                    await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS work_statistics (
                    id                          INTEGER(12)     UNSIGNED    NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    program_name                VARCHAR(100)    NOT NULL,
                    started_at                  DATETIME        NOT NULL DEFAULT NOW(),
                    last_successful_at          DATETIME        NULL,
                    duration                    VARCHAR(100)    NOT NULL DEFAULT ''
                    )""")
                await conn.commit()

                if 'inactive_cars_sent_av_by' not in existing_tables:
                    await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS inactive_cars_sent_av_by (
                    id                          INTEGER(12)     UNSIGNED    NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    cars_id                     INTEGER(12)     UNSIGNED    NOT NULL,
                    sent_at                     DATETIME                    NOT NULL,
                    is_send                     TINYINT(1)                  NOT NULL  DEFAULT 0
                    )""")
                await conn.commit()

        pool.close()
        await pool.wait_closed()

    async def start_work_statistics(self, program_name: str,
                                    started_at: Union[None, str, datetime.datetime] = None) -> None:
        if started_at is None:
            started_at = datetime.datetime.now()
        started_at = str(started_at)
        parameters = (program_name, started_at)
        stmt = """INSERT INTO work_statistics (program_name, started_at) VALUE (%s, %s)"""
        await self.execute(stmt, parameters)

    async def update_work_statistics(self, program_name: str, last_successful_at: Union[str, datetime.datetime],
                                     duration: str) -> None:
        last_successful_at = str(last_successful_at)

        search_sql = """SELECT id FROM work_statistics WHERE program_name LIKE %s ORDER BY started_at DESC LIMIT 1"""
        last_program_id: Optional[tuple] = await self.fetchone(search_sql, program_name)
        if last_program_id is None:
            return await self.start_work_statistics(program_name=program_name)

        last_program_id = last_program_id[0]
        parameters = (last_successful_at, duration, last_program_id)
        update_sql = """UPDATE work_statistics SET last_successful_at=%s, duration=%s WHERE id=%s"""
        await self.execute(update_sql, parameters)

    async def get_equipment(self) -> (dict, dict):
        equipment = dict()
        equipment_group = dict()
        stmt = """SELECT group_name, en_name, ru_name FROM equipment"""
        pool = await aiomysql.create_pool(**self.pool_params)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(stmt)
                result = await cursor.fetchall()
                for group_name, en_name, ru_name in result:
                    equipment[en_name] = ru_name
                    equipment_group[en_name] = group_name
        pool.close()
        await pool.wait_closed()
        return equipment, equipment_group

    async def get_exist_car(self, ad_id: str):
        stmt = f"""SELECT Price_rub, Vin, Phone, Photos FROM cars WHERE ad_id LIKE '{ad_id}' """
        pool = await aiomysql.create_pool(**self.pool_params)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(stmt)
                result = await cursor.fetchone()
        pool.close()
        await pool.wait_closed()
        return result

    async def count_elem(self, table_name) -> int:
        stmt = f"""SELECT COUNT(*) FROM {table_name}"""
        pool = await aiomysql.create_pool(**self.pool_params)
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(stmt)
                count = await cursor.fetchone()
                if count:
                    count = count[0]
                else:
                    count = 0
        pool.close()
        await pool.wait_closed()
        return count

    async def update_price(self, update_list):
        """update_list = (Price_rub, Price_dollar, ad_id)."""

        update_sql = """UPDATE cars SET Price_rub=%s, Price_dollar=%s, 
                        num_changes=num_changes + 1, updated_at=NOW(), is_active=1, was_sent=0
                        WHERE ad_id LIKE %s """
        await self.executemany(sql_query_template=update_sql, list_values=update_list)

    async def update_images(self, table_name: str, update_list):
        update_sql = f"""UPDATE {table_name} 
        SET Photos=%s, num_changes=num_changes + 1, updated_at=NOW(), is_active=1, was_sent=0
        WHERE ad_id LIKE %s """
        await self.executemany(sql_query_template=update_sql, list_values=update_list)

    async def get_last_sent_at(self, table_name, ad_id) -> Optional[str]:
        sql = f"""SELECT last_sent_at FROM {table_name} WHERE ad_id LIKE %s """
        result = await self.fetchone(sql_query=sql, list_values=ad_id)
        last_sent_at = result
        if result:
            last_sent_at = result[0]
        return last_sent_at

    async def set_ad_as_raised(self, table_name:str, ad_id: str, raised_dt: str):
        list_values = [raised_dt, ad_id]
        sql = f"""UPDATE {table_name} SET ad_was_raised=%s, is_active=1, was_sent=0 
        WHERE ad_id LIKE %s """
        await self.execute(sql_query=sql, list_values=list_values)

    async def mark_ad_as_sent(self, table_name:str, post_data_str: str, ad_id: str):
        list_values = [post_data_str, ad_id]
        sql = f"""UPDATE {table_name} SET last_sent_data=%s, last_sent_at=NOW(), was_sent=1 
        WHERE ad_id LIKE %s """
        await self.execute(sql_query=sql, list_values=list_values)

    async def get_never_checked_ads_id(self, table_name: str, limit: int = 500):
        sql = f"""SELECT id FROM {table_name}
                    WHERE is_active=1 AND date_of_check IS NULL AND DATE(parsed_at) < CURRENT_DATE()
                    ORDER BY id   
                    LIMIT {limit} """
        result = await self.fetchall(sql)
        never_checked_id_list = [e[0] for e in result]
        return never_checked_id_list

    async def get_not_checked_data(self, table_name, db_id):
        sql = f"""SELECT ad_id, Site_link, Price_rub FROM {table_name} WHERE id={db_id} """
        ad_id, public_url, price_byn = await self.fetchone(sql)
        return ad_id, public_url, price_byn

    async def set_ad_as_checked(self, table_name, check_data):
        # check_data = (Price_rub, Price_dollar, date_of_check, is_active, date_sending_check, ad_id)
        is_active = check_data[3]
        ad_id = check_data[5]
        price_byn = check_data[0]

        if is_active and price_byn:
            sql = f"""UPDATE {table_name} 
                      SET Price_rub=%s, Price_dollar=%s, date_of_check=%s, is_active=%s, date_sending_check=%s 
                      WHERE ad_id LIKE %s """
        else:
            date_of_check = check_data[2]
            date_sending_check = check_data[4]
            if date_sending_check:
                check_data = (date_of_check, is_active, date_sending_check, ad_id)
                sql = f"""UPDATE {table_name} 
                          SET date_of_check=%s, is_active=%s, date_sending_check=%s
                          WHERE ad_id LIKE %s """
            else:
                check_data = (date_of_check, is_active, ad_id)
                sql = f"""UPDATE {table_name} 
                          SET date_of_check=%s, is_active=%s
                          WHERE ad_id LIKE %s """

        await self.execute(sql, check_data)

    async def update_checked_data(self, table_name: str, list_check_data: list):
        list_changes_price = []
        list_not_active = []
        list_no_change = []

        changes_price_sql = f"""UPDATE {table_name} 
                  SET Price_rub=%s, Price_dollar=%s, date_of_check=%s, is_active=%s, date_sending_check=%s 
                  WHERE ad_id LIKE %s """
        not_active_sql = f"""UPDATE {table_name} 
                             SET date_of_check=%s, is_active=%s, date_sending_check=%s
                             WHERE ad_id LIKE %s """
        no_change_sql = f"""UPDATE {table_name} 
                            SET date_of_check=%s, is_active=%s
                            WHERE ad_id LIKE %s """

        for check_data in list_check_data:
            # check_data = (Price_rub, Price_dollar, date_of_check, is_active, date_sending_check, ad_id)
            is_active = check_data[3]
            ad_id = check_data[5]
            price_byn = check_data[0]

            if is_active and price_byn:
                # Если цена изменилась
                list_changes_price.append(check_data)
            else:
                date_of_check = check_data[2]
                date_sending_check = check_data[4]
                if date_sending_check:
                    # Если стало неактивным
                    check_data = (date_of_check, is_active, date_sending_check, ad_id)
                    list_not_active.append(check_data)
                else:
                    # Если ничего не поменялось
                    check_data = (date_of_check, is_active, ad_id)
                    list_no_change.append(check_data)

        if list_changes_price:
            await self.executemany(changes_price_sql, list_changes_price)
        if list_not_active:
            await self.executemany(not_active_sql, list_not_active)
        if list_no_change:
            await self.executemany(no_change_sql, list_no_change)

    async def get_earlier_checked_ads_id(self, table_name: str, limit: int = 500):
        sql = f"""SELECT id FROM {table_name}
                  WHERE is_active=1 AND date_of_check IS NOT NULL AND DATE(date_of_check) < CURRENT_DATE()
                  ORDER BY id  
                  LIMIT {limit} """
        result = await self.fetchall(sql)
        earlier_checked_id_list = [e[0] for e in result]
        return earlier_checked_id_list

    async def get_unsent_ad_id(self, table_name: str, limit: int = 500):
        sql = f"""SELECT ad_id, id FROM {table_name} 
        WHERE DATE(date_of_check) < DATE('2021-07-23') AND is_active=0
        LIMIT {limit}
        """
        result = await self.fetchall(sql)
        unsent_ad_id_list = [e[0] for e in result]
        db_id_list = [e[1] for e in result]
        return unsent_ad_id_list, db_id_list

    async def select_inactive_unsent_cars(self, table_name: str, dependent_table: str, limit: int = 500) -> tuple:
        """Выбираем не активные авто, которые ещё не отправляли."""

        # sql = f"""
        # SELECT t1.* FROM
        #     (SELECT * FROM {table_name} WHERE is_active=0) as t1
        #     LEFT JOIN {dependent_table} as t2
        #         on t1.id=t2.cars_id
        # WHERE t2.cars_id IS NULL
        # LIMIT {limit}
        # """
        sql = f"""
        SELECT * FROM {table_name} 
        WHERE is_active=0 AND id NOT IN (SELECT cars_id FROM {dependent_table}) 
        LIMIT {limit}
        """
        return await self.fetchall(sql)

    async def mark_inactive_car_as_sent(self, dependent_table: str, list_values: List[tuple]) -> None:
        """Отмечаем отправленные неактивные машины."""

        # list_value = [(cars_id, sent_at), (cars_id, sent_at), ...]
        sql = f"""INSERT INTO {dependent_table} (cars_id, sent_at, is_send) VALUE (%s, %s, %s) """
        await self.executemany(sql, list_values)

    async def select_already_sent_inactive_cars(self, table_name: str, dependent_table: str, limit: int = 500) -> tuple:
        """Выбираем неактивные машины, которые уже отправили."""

        sql = f"""
        SELECT t2.* FROM
            (SELECT cars_id FROM {dependent_table} WHERE is_send=0 LIMIT {limit}) as t1
            JOIN {table_name} as t2
                on t1.cars_id=t2.id
        """
        return await self.fetchall(sql)

    async def mark_already_sent_inactive_car_as_sent(self, dependent_table: str, list_values: List[tuple]) -> None:
        """Отмечаем отправленные неактивные машины."""

        # list_value = [(sent_at, is_send, cars_id), (sent_at, is_send, cars_id), ...]
        sql = f"""UPDATE {dependent_table} SET sent_at=%s, is_send=%s WHERE cars_id=%s """
        await self.executemany(sql, list_values)


def init_db():
    db_config = config.db_config
    if CONNECT_TO_REMOTELY_DB:
        db_config = config.db_remotely
    return DBServices(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
    )
