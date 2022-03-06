import asyncio
import datetime
import json
import os
import random
from pathlib import Path
from typing import Optional

import aiohttp
from async_timeout import timeout

from common.errors import DetailInfoError, ParserError, Error502, ReloadSession
from common.log import create_logger
from models import init_db

logger = create_logger(__file__)

start = datetime.datetime.now()
program_name = Path(__file__).stem
db = init_db()


class SendInactiveCarsAvBy:

    def __init__(self):
        self.num_of_attempts = 10
        self.sleep_time = 5
        self.time_out = 60
        self.delay: int = 0
        self.pool_threads: int = 10

        self.logger = logger.getChild(self.__class__.__name__)

        self.aiosession: Optional[aiohttp.ClientSession] = None
        self.site = 'av_by'
        self.table_name = 'cars'
        self.dependent_table = 'inactive_cars_sent_av_by'
        self.post_host = '146.158.13.28'
        self.post_url = 'https://146.158.13.28/parcer_auto/index.php'
        # self.post_url = 'https://146.158.13.28/parcer_auto/deactive.php'

        self.limit: int = 500
        self.update_list = []
        self.list_dict_data_for_sending = []
        self.folder_files_json = 'files_json/'
        self.count_send = 0

    async def fetch(self, url=None, method='get', **kwargs):

        for i in range(self.num_of_attempts):
            try:
                async with timeout(self.time_out):
                    async with getattr(self.aiosession, method)(url, **kwargs) as response:
                        if response.status == 200:
                            return await response.read()
                        elif response.status == 404:
                            self.logger.debug("ERROR 404 (Page Not Found): %s", url)
                            return 404
                        elif response.status >= 500:
                            status = response.status
                            self.logger.warning(f'[fetch] Response {status}. {url}')
                            raise Error502(f'Response {status}')
                        else:
                            self.logger.error(f"ResponseStatus {response.status}: %s", url)
                            return response.status
            except asyncio.TimeoutError:
                self.logger.debug("TIMEOUT: %s", url)

            await asyncio.sleep(random.uniform(10, self.sleep_time * 10) / 10)

    @staticmethod
    async def write_to_work_statistics():
        now = datetime.datetime.now()
        total_diff = now - start
        total_time = str(total_diff).split('.')[0]
        await db.update_work_statistics(
            program_name=program_name, last_successful_at=now, duration=total_time)

    def write_to_file(self, file_name, data: dict, mode='w+', indent=4):
        with open(f'{self.folder_files_json}{file_name}.json', f'{mode}') as f:
            data = json.dumps(data, indent=indent, ensure_ascii=False)
            f.write(data)
            f.write('\n')

    async def bound_send_data(self, cars_id, dict_data, sem):
        """Отправляем результат пакетами. По умолчанию по 10 шт за раз."""
        async with sem:
            return await self.sent_post_request(cars_id=cars_id, dict_data=dict_data)

    async def sent_post_request(self, cars_id: int, dict_data: dict) -> None:
        """Отправка данных на сайт carprice.by."""

        not_null_key = [
            'Phone',
            'Year',
            'Brand',
            'Model',
        ]
        is_send = 0

        try:
            for key, val in dict_data.items():
                # Если значение ключа не может быть нулевым
                if key in not_null_key and not val:
                    sent_at = str(datetime.datetime.now()).split('.')[0]
                    self.logger.warning(f'Dont sent id = {cars_id = }, {key} = {val}')
                    self.update_list.append((cars_id, sent_at, is_send))
                    return

            while True:
                today = datetime.date.today()
                sent_at = str(datetime.datetime.now()).split('.')[0]
                try:
                    resp = await self.fetch(self.post_url, method='post', data=dict_data)
                    # resp = '\n\r'

                    # print(f'{dict_data = }')
                    # print(resp)
                    # print(repr(resp))
                    is_send = 1
                    self.count_send += 1
                    self.logger.debug(f'[post] {self.count_send = }')
                except Error502 as e:
                    error_message = str(e)
                    self.logger.error(f'[post] {error_message} Can not send id = {cars_id = }')
                    if '502' in error_message:
                        await asyncio.sleep(300)
                    else:
                        await asyncio.sleep(60)
                    return await self.sent_post_request(cars_id, dict_data)
                except aiohttp.ClientConnectorError:
                    # Когда сервак недоступен - перезагружается
                    self.logger.error(f'Сервер {self.post_url} недоступен.')
                    await asyncio.sleep(15)
                    raise ReloadSession()

                if resp is not None and not isinstance(resp, int):
                    break
                elif isinstance(resp, int):
                    self.logger.error(f'[post] Response = {resp} Can not send id = {cars_id = }')
                    return
                else:
                    self.logger.warning(f'[post] Trying to send again id = {cars_id = }')
                    self.write_to_file(f'Try_send_{program_name}_{today}', dict_data, 'a+')

            self.update_list.append((cars_id, sent_at, is_send))

        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def create_dict_for_send(self, tuple_data: tuple) -> (int, dict):
        """Создание словаря данных для отправки."""

        try:
            # dict_for_send = dict()

            # for i, item in enumerate(tuple_data):
            #     print(f'{i} = {item}')

            cars_id = tuple_data[0]
            ad_id = tuple_data[1]
            title = tuple_data[2]                                                   # <class 'str'>: Car_name
            # phone = tuple_data[3]                                                   # <class 'list'>: Phone
            phone = json.loads(tuple_data[3].replace("'", '"')) or []               # <class 'list'>: Phone
            price_byn = tuple_data[4]                                               # <class 'str'>: Price_rub
            price_usd = tuple_data[5]                                               # <class 'str'>: Price_dollar
            year = tuple_data[6]                                                    # <class 'str'>: Year
            vin = tuple_data[7]                                                     # <class 'str'>: Vin
            mileage_km = tuple_data[8]                                              # <class 'str'>: Сar_mileage
            engine_type = tuple_data[9]                                             # <class 'str'>: Fuel_type: дизель
            engine_capacity = tuple_data[10]                                        # <class 'str'>: Volume: 2.2
            color = tuple_data[11]                                                  # <class 'str'>: Colour: зелёный
            body_type = tuple_data[12]                                              # <class 'str'>: Car_Type: микроавтобус пассажирский
            transmission_type = tuple_data[13]                                      # <class 'str'>: Transmission: механика
            drive_type = tuple_data[14]                                             # <class 'str'>: Drive_Unit: задний привод
            photos = tuple_data[15]                                                 # <class 'str'>: Photos:
            public_url = tuple_data[16]                                             # <class 'str'>: Site_link: https://cars.av.by/mercedes-benz/vito/100526505
            published_data = tuple_data[18]                                         # <class 'str'>: Publish_date: 2021-08-07
            seller_name = tuple_data[19]                                            # <class 'str'>: Owner_name: Евгений
            location_name = tuple_data[20]                                          # <class 'str'>: Owner_city: Хойники, Гомельская обл.
            description = tuple_data[21]                                            # <class 'str'>: Description:
            # equipment_list = tuple_data[22]                                         # <class 'list'>: Auto_info: ['фаркоп',...]
            equipment_list = json.loads(tuple_data[22].replace("'", '"')) or []     # <class 'list'>: Auto_info: ['фаркоп',...]
            brand = tuple_data[23]                                                  # <class 'str'>: Brand: Mercedes-Benz
            model = tuple_data[24]                                                  # <class 'str'>: Model: Vito
            vin_checked = bool(tuple_data[25])                                      # <class 'bool'>: Vin_Checked: False
            condition = tuple_data[26]                                              # <class 'str'>: Condition_car: с пробегом
            number_of_seats = tuple_data[27]                                        # <class 'str'>: Number_of_seats:
            short_location_name = tuple_data[28]                                    # <class 'str'>: ShortLocation: Хойники
            generation = tuple_data[29]                                             # <class 'str'>: Generation: W639
            generation_with_years = tuple_data[30]                                  # <class 'str'>: Generation_with_years: W639 (2003 - 2010)
            refreshed_at = tuple_data[31]                                           # <class 'str'>: Refreshed_at: 2021-08-07T04:46:16+0000

            dict_for_send = {
                'Car_name': title,
                'Phone': phone,
                'Price_rub': price_byn,
                'Price_dollar': price_usd,
                'Year': year,
                'Vin': vin,
                'Сar_mileage': mileage_km,
                'Fuel_type': engine_type,
                'Volume': engine_capacity,
                'Colour': color,
                'Car_Type': body_type,
                'Transmission': transmission_type,
                'Drive_Unit': drive_type,
                'Photos': photos,
                'Site_link': public_url,
                'Site_number': ad_id,
                'Publish_date': published_data,
                'Owner_name': seller_name,
                'Owner_city': location_name,
                'Description': description,
                'Auto_info': equipment_list,
                'Brand': brand,
                'Model': model,

                'Vin_Checked': vin_checked,
                'Condition_car': condition,
                'Number_of_seats': number_of_seats,
                'ShortLocation': short_location_name,
                'Generation': generation,
                'Generation_with_years': generation_with_years,
                'Refreshed_at': refreshed_at,
                'ad_was_raised': None,
                'Auto_info2[]': equipment_list,
                'is_active': 0,
            }

            # for key, val in dict_for_send.items():
            #     print(f'{type(val)}: {key}: {val}')

            # print(json.dumps(dict_for_send, indent=4, ensure_ascii=False))
            return (cars_id, dict_for_send)
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def send_inactive_cars(self):
        """Отправляем не активные авто, которые ещё не отправляли."""
        try:
            # print('='*50)
            inactive_unsent_cars_tuple = await db.select_inactive_unsent_cars(table_name=self.table_name,
                                                                              dependent_table=self.dependent_table,
                                                                              limit=self.limit)

            while inactive_unsent_cars_tuple:
                logger.debug(f'inactive_unsent_cars_tuple = {len(inactive_unsent_cars_tuple)}')
                self.list_dict_data_for_sending = []
                self.update_list = []
                self.count_send = 0

                for tuple_data in inactive_unsent_cars_tuple:
                    cars_id, dict_data = await self.create_dict_for_send(tuple_data)
                    self.list_dict_data_for_sending.append((cars_id, dict_data))
                    # break
                # break
                logger.debug(f'Need send = {len(self.list_dict_data_for_sending)}')
                # #####################################################################################################
                # Модуль отправки
                try:
                    sem = asyncio.Semaphore(self.pool_threads)
                    tasks_for_sending = []
                    for cars_id, dict_data in self.list_dict_data_for_sending:
                        task = asyncio.create_task(self.bound_send_data(cars_id, dict_data, sem))
                        tasks_for_sending.append(task)
                    await asyncio.gather(*tasks_for_sending)
                except aiohttp.ClientConnectorError as e:
                    self.logger.error(DetailInfoError(e, f'{self.post_host} Client Connect Error'))
                    await asyncio.sleep(60)
                    raise ReloadSession()

                # Пишем в БД, что программа работает
                await self.write_to_work_statistics()

                if self.update_list:
                    # update_list = [(cars_id, sent_at, is_send), (cars_id, sent_at, is_send), ...]
                    await db.mark_inactive_car_as_sent(dependent_table=self.dependent_table,
                                                       list_values=self.update_list)
                else:
                    raise ParserError('No self.update_list !!!')

                self.logger.info(f'[send_inactive_cars] last_db_id = {inactive_unsent_cars_tuple[-1][0]}')
                # self.logger.debug(f'[send_inactive_cars] self.count_send = {self.count_send}')

                inactive_unsent_cars_tuple = await db.select_inactive_unsent_cars(table_name=self.table_name,
                                                                                  dependent_table=self.dependent_table,
                                                                                  limit=self.limit)

                # break
            self.list_dict_data_for_sending = []
            self.update_list = []
            self.count_send = 0
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise
        finally:
            if self.update_list:
                await db.mark_inactive_car_as_sent(dependent_table=self.dependent_table,
                                                   list_values=self.update_list)
            self.list_dict_data_for_sending = []
            self.update_list = []
            self.count_send = 0

    async def run(self):
        """Entry point."""

        try:
            if not os.path.exists(self.folder_files_json):
                os.makedirs(self.folder_files_json)

            if self.aiosession:
                await self.aiosession.close()
                self.aiosession = None

            user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                         'Chrome/88.0.4324.150 Safari/537.36'
            headers = {
                'User-Agent': user_agent,
            }
            conn = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=conn, headers=headers) as self.aiosession:

                await self.send_inactive_cars()


            await self.write_to_work_statistics()

        except asyncio.CancelledError:
            return
        except ReloadSession:
            self.logger.warning('Reload Session ...')
            await asyncio.sleep(5)
            await self.run()
        except Error502 as e:
            error_message = str(e)
            self.logger.warning(DetailInfoError(e, error_message))
            if '502' in error_message:
                await asyncio.sleep(300)
            else:
                await asyncio.sleep(60)
            await self.run()
        except aiohttp.ClientConnectorError as e:
            self.logger.error(DetailInfoError(e, 'Client Connect Error'))
            await asyncio.sleep(5)
            await self.run()
        except aiohttp.ServerDisconnectedError as e:
            self.logger.error(DetailInfoError(e, 'Server Connect Error'))
            await asyncio.sleep(5)
            await self.run()
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise
        finally:
            if self.aiosession:
                await self.aiosession.close()



async def main():
    """Отправка неактивных объявлений. После косяка Ивана.
    Когда он в результате чека удалял авто, вместо того, чтоб отметить как не активные."""

    await db.create_db_and_tables()
    await db.start_work_statistics(program_name=program_name, started_at=start)

    obj = SendInactiveCarsAvBy()
    await obj.run()


if __name__ == '__main__':
    logger.info('Начало.')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.critical(DetailInfoError(e, 'СБОЙ РАБОТЫ ПРОГРАММЫ.'))
    finally:
        end = str(datetime.datetime.now() - start).split('.')[0]
        logger.info('Конец. Длительность: %s\n;;;;', end)