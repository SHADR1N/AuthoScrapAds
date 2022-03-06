import asyncio
import copy
import datetime
import json
import os
import random
from pathlib import Path
from typing import Optional

import aiohttp
from async_timeout import timeout
from lxml.html import document_fromstring

from common.errors import DetailInfoError, ParserError, Error502, ReloadSession
from common.log import create_logger
from models import init_db

logger = create_logger(__file__)

start = datetime.datetime.now()
program_name = Path(__file__).stem
db = init_db()


class CheckAvBy:

    def __init__(self):
        self.num_of_attempts = 10
        self.sleep_time = 5
        self.time_out = 30
        self.delay: int = 0
        self.pool_threads: int = 10

        self.logger = logger.getChild(self.__class__.__name__)

        self.aiosession: Optional[aiohttp.ClientSession] = None
        self.site = 'av_by'
        self.table_name = 'cars'
        self.set_delete_ad_ad = set()
        self.set_changed_ad_ad = set()
        self.post_host = '146.158.13.28'
        # self.post_url = 'https://146.158.12.240/parcer_auto/update.php'   # основной сервер
        self.post_url = 'https://146.158.13.28/parcer_auto/update.php'

        self.limit_checking_pages_at_a_time: int= 500
        self.list_dict_data_for_sending = []
        self.update_list = []
        self.count_send = 0

    async def fetch(self, url=None, method='get', **kwargs):

        if self.delay:
            await asyncio.sleep(random.uniform(10, self.delay * 10) / 10)

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
                # pass

            await asyncio.sleep(random.uniform(10, self.sleep_time * 10) / 10)

    async def sent_post_request(self, dict_data):
        """Отправляем на сайт carprice.by. После помечаем как проверенные в БД"""

        try:
            ad_id = dict_data['ad_id']
            is_active = dict_data['is_active']
            change_price = dict_data['change_price']
            date_sending_check = None
            date_of_check = str(datetime.datetime.now()).split('.')[0]

            # Отправляем если перестало быть активным или если поменялась цена
            if is_active == 0 or is_active == 1 and change_price is not None:
                self.aiosession._default_headers['Content-Type'] = 'application/x-www-form-urlencoded'
                while True:
                    today = datetime.date.today()

                    if change_price:
                        date_of_check = str(datetime.datetime.now()).split('.')[0]
                        dict_data['change_price']['price_dt'] = date_of_check
                    try:

                        modified_dict_data = copy.deepcopy(dict_data)
                        if isinstance(modified_dict_data.get('change_price'), dict):
                            dict_change_price: dict = modified_dict_data.pop('change_price')
                            for key, value in dict_change_price.items():
                                modified_dict_data[f'change_price[{key}]'] = value
                        else:
                            modified_dict_data['change_price'] = 'null'

                        resp = await self.fetch(self.post_url, method='post', data=modified_dict_data)
                        # resp = '\n\r'
                        # print('ОТПРАВЛЕНО', modified_dict_data, self.post_url)
                        # print('resp =', resp)

                        self.write_to_file(f'{program_name}_{today}', dict_data, 'a+')
                        self.count_send += 1
                        self.logger.debug(f'[post] {self.count_send = }')

                    except Error502 as e:
                        error_message = str(e)
                        self.logger.error(f'[post] {error_message} Can not send {ad_id}')
                        if '502' in error_message:
                            await asyncio.sleep(300)
                        else:
                            await asyncio.sleep(60)
                        return await self.sent_post_request(dict_data)

                    if resp is not None and not isinstance(resp, int):
                        break
                    elif isinstance(resp, int):
                        self.logger.error(f'[post] Response = {resp} Can not send {ad_id}')
                        return
                    else:
                        self.logger.warning(f'[post] Trying to send again {ad_id}')
                        self.write_to_file(f'Try_send_{program_name}_{today}', dict_data, 'a+')

                date_sending_check = date_of_check

            price_byn = None
            price_usd = None

            if is_active and change_price:
                price_byn = dict_data['change_price']['Price_rub']
                price_usd = dict_data['change_price']['Price_dollar']

            check_data = (price_byn, price_usd, date_of_check, is_active, date_sending_check, ad_id)
            self.update_list.append(check_data)
            # await db.set_ad_as_checked(table_name=self.table_name, check_data=check_data)

        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def get_dict_data_for_db_id(self, db_id: int):
        """Получение данных с сайта по его id в БД."""

        try:
            ad_id, public_url, db_price_byn = await db.get_not_checked_data(
                table_name=self.table_name, db_id=db_id)

            resp = await self.fetch(public_url)

            if resp and isinstance(resp, bytes):
                page = resp.decode()
                tree = document_fromstring(page)
                next_data = tree.find('.//script[@id="__NEXT_DATA__"]')
                text = next_data.text_content()
                dict_data = json.loads(text)
                ad = dict_data['props']['initialState']['advert']['advert']
                # self.write_to_file(file_name='dict_data_from_page', data=ad)
                status = ad['status']
                if status == 'active':  # активно
                    is_active = 1
                elif status in ('removed', 'archived'):  # Продано
                    is_active = 0
                elif status == 'paused':  # Временно недоступно. А значит пока удалять не надо
                    is_active = 1
                elif status == 'premoderation':  # активное, но что-то там на модерации
                    is_active = 1
                elif status == 'rejected':
                    is_active = 0
                else:
                    self.logger.warning(f'[data] Unknown status = {status}')
                    raise ParserError(f'[data] Не удалось найти статус для {public_url}')
                price = ad['price']
                site_price_byn = str(price['byn']['amount'])
                site_price_usd = str(price['usd']['amount'])

                if is_active and site_price_byn != db_price_byn:
                    # price_dt = str(datetime.datetime.now()).split('.')[0]
                    dict_data = {
                        "site": self.site,
                        "ad_id": ad_id,
                        "is_active": 1,
                        "change_price": {
                            # "price_dt": price_dt,
                            "Price_rub": site_price_byn,
                            "Price_dollar": site_price_usd,
                        }
                    }
                    self.set_changed_ad_ad.add(ad_id)
                elif not is_active:
                    dict_data = {
                        "site": self.site,
                        "ad_id": ad_id,
                        "is_active": 0,
                        "change_price": None
                    }
                    self.set_delete_ad_ad.add(ad_id)
                else:
                    dict_data = {
                        "site": self.site,
                        "ad_id": ad_id,
                        "is_active": 1,
                        "change_price": None
                    }

            elif isinstance(resp, int) and resp == 404:
                # значит объявление пропало с сайта. Отмечаем как проданное
                dict_data = {
                    "site": self.site,
                    "ad_id": ad_id,
                    "is_active": 0,
                    "change_price": None
                }
                self.set_delete_ad_ad.add(ad_id)
            else:
                raise ParserError(f'[data] Ответ сервера {resp} для {public_url}')

            if dict_data:
                self.list_dict_data_for_sending.append(dict_data)

        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def bound_get_dict_data(self, db_id, sem):
        """Быстрый парсинг страниц с av.by."""
        async with sem:
            return await self.get_dict_data_for_db_id(db_id=db_id)

    async def bound_send_data(self, dict_data, sem):
        """Отправляем результат пакетами. По умолчанию по 10 шт за раз."""
        async with sem:
            return await self.sent_post_request(dict_data=dict_data)

    async def check_never_checked(self):
        """Проверяем те, которые до этого ни разу не проверяли."""

        try:
            # Получаем список ID активных объявлений которые ещё не проверялись ни разу
            never_checked_id_list = await db.get_never_checked_ads_id(table_name=self.table_name,
                                                                      limit=self.limit_checking_pages_at_a_time)
            # print('never_checked_id_list =', never_checked_id_list)

            while never_checked_id_list:
                self.list_dict_data_for_sending = []
                self.update_list = []
                self.count_send = 0

                # print('never_checked_id_list =', never_checked_id_list)
                sem = asyncio.Semaphore(self.pool_threads)

                # Получаем данные с сайта
                tasks_data = []
                for db_id in never_checked_id_list:
                    task = asyncio.create_task(self.bound_get_dict_data(db_id, sem))
                    tasks_data.append(task)

                await asyncio.gather(*tasks_data)
                # Пишем в БД, что программа работает
                await self.write_to_work_statistics()

                self.logger.debug(f'[never_checked] Спарсилось = {len(self.list_dict_data_for_sending)}')

                # на случай если сервер перезагружается, чтоб не долбить его постоянно
                try:
                    tasks_for_sending = []
                    for dict_data in self.list_dict_data_for_sending:
                        task = asyncio.create_task(self.bound_send_data(dict_data, sem))
                        tasks_for_sending.append(task)
                    await asyncio.gather(*tasks_for_sending)
                except aiohttp.ClientConnectorError as e:
                    self.logger.error(DetailInfoError(e, f'{self.post_host} Client Connect Error'))
                    await asyncio.sleep(60)
                    raise ReloadSession()

                self.logger.debug(f'[never_checked] Отправилось = {self.count_send}')

                # Пишем в БД, что программа работает
                await self.write_to_work_statistics()

                if self.update_list:
                    # update_list = [(price_byn, price_usd, date_of_check, is_active, date_sending_check, ad_id), ...]
                    await db.update_checked_data(table_name=self.table_name, list_check_data=self.update_list)
                else:
                    raise ParserError('No self.update_list !!!')

                self.logger.info(
                    f'[never_checked] last_db_id = {never_checked_id_list[-1]}')

                never_checked_id_list = await db.get_never_checked_ads_id(table_name=self.table_name,
                                                                          limit=self.limit_checking_pages_at_a_time)

            self.list_dict_data_for_sending = []
            self.update_list = []
            self.count_send = 0

        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def check_earlier_checked(self):
        """Проверяем всё, что было проверено ранне чем сегодняшняя дата"""

        try:
            # Получаем список ID активных объявлений которые ещё не проверяли сегодня
            earlier_checked_id_list = await db.get_earlier_checked_ads_id(table_name=self.table_name,
                                                                          limit=self.limit_checking_pages_at_a_time)
            # print('earlier_checked_id_list =', earlier_checked_id_list)

            while earlier_checked_id_list:
                self.list_dict_data_for_sending = []
                self.update_list = []
                self.count_send = 0

                # print('earlier_checked_id_list =', earlier_checked_id_list)
                sem = asyncio.Semaphore(self.pool_threads)

                # Получаем данные с сайта
                tasks_data = []
                for db_id in earlier_checked_id_list:
                    task = asyncio.create_task(self.bound_get_dict_data(db_id, sem))
                    tasks_data.append(task)

                await asyncio.gather(*tasks_data)
                # Пишем в БД, что программа работает
                await self.write_to_work_statistics()

                self.logger.debug(f'[earlier_checked] Спарсилось = {len(self.list_dict_data_for_sending)}')

                # на случай если сервер перезагружается, чтоб не долбить его постоянно
                try:
                    tasks_for_sending = []
                    for dict_data in self.list_dict_data_for_sending:
                        task = asyncio.create_task(self.bound_send_data(dict_data, sem))
                        tasks_for_sending.append(task)
                    await asyncio.gather(*tasks_for_sending)
                except aiohttp.ClientConnectorError as e:
                    self.logger.error(DetailInfoError(e, f'{self.post_host} Client Connect Error'))
                    await asyncio.sleep(60)
                    raise ReloadSession()

                # Пишем в БД, что программа работает
                await self.write_to_work_statistics()

                self.logger.debug(f'[earlier_checked] Отправилось = {self.count_send}')

                if self.update_list:
                    # update_list = [(price_byn, price_usd, date_of_check, is_active, date_sending_check, ad_id), ...]
                    await db.update_checked_data(table_name=self.table_name, list_check_data=self.update_list)
                else:
                    raise ParserError('No self.update_list !!!')

                self.logger.info(f'[earlier_checked] last_db_id = {earlier_checked_id_list[-1]}')

                earlier_checked_id_list = await db.get_earlier_checked_ads_id(table_name=self.table_name,
                                                                              limit=self.limit_checking_pages_at_a_time)
                # break

            self.list_dict_data_for_sending = []
            self.update_list = []
            self.count_send = 0

        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

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

    async def send_test_request(self, data: Optional[dict] = None):
        """Пробная Отправка данных."""

        self.aiosession.headers.update(
            # {'Content-Type': 'application/x-www-form-urlencoded; boundary='},
            {'Content-Type': 'application/x-www-form-urlencoded'},
        )
        # data = {'site': 'av_by', 'ad_id': '100436461', 'is_active': 0, 'change_price': None}
        data = {
            "site": "av_by",
            "ad_id": "100464696",
            "is_active": 1,
            "change_price": {
                "Price_rub": "123",
                "Price_dollar": "4000",
                "price_dt": "2021-07-16 09:45:36",
            },
            "TEST": ["a", "b"]
        }
        # data = {
        #     # "site": "av_by",
        #     # "ad_id": "100394580",
        #     # "is_active": 1,
        #     # # 'change_price[0][Price_rub]': '22583',
        #     # # 'change_price[0][Price_dollar]': '8800',
        #     # # 'change_price[0][price_dt]': '22583',
        #     # # 'change_price[1][Price_rub]': '22583',
        #     # # 'change_price[1][Price_dollar]': '8800',
        #     # # 'change_price[1][price_dt]': '22583',
        #     # # 'change_price[price_dt]': '22583',
        #     # # 'change_price[]': [],
        #     #
        #     # 'change_price[Price_rub]': '22583',
        #     # 'change_price[Price_dollar]': '8800',
        #     # 'change_price[price_dt]': '22583',
        #     'Auto_info2[]': ['легкосплавные диски', 'ABS', 'ESP', 'антипробуксовочная ', 'иммобилайзер', 'передние', 'боковые', 'задние', 'датчик дождя', 'камера заднего вида', 'парктроники', 'тёмный', 'комбинированные материалы', 'круиз-контроль', 'управление мультимедиа с руля', 'электрорегулировка сидений', 'передние электро-стеклоподъёмники', 'задние электро-стеклоподъёмники', 'сидений', 'зеркал', 'руля', 'климат-контроль', 'кондиционер', 'AUX или iPod', 'Bluetooth', 'CD или MP3', 'USB', 'мультимедийный экран', 'штатная навигация', 'светодиодные']
        # }
        # change_price = data['change_price']
        # list_change_price = []
        # if change_price:
        #     list_change_price.append(change_price)
        #     data['change_price'] = list_change_price

        # data = json.dumps(data, ensure_ascii=False, indent=4)
        # with aiohttp.MultipartWriter('mixed') as mpwriter:
        #     mpwriter.append_json(data)
        #     resp = await self.fetch(self.post_url, method='post', data=mpwriter)
        #     print(resp)
        self.post_url = 'https://146.158.12.240/parcer_auto/test.php'
        resp = await self.fetch(self.post_url, method='post', data=data)
        # resp = await self.fetch(self.post_url, method='post', json=data)
        print(resp)

        self.logger.debug('[TEST] SUCCESSFULLY !!!')

    async def run(self):
        """Entry point."""

        try:
            self.folder_files_json = 'files_json/'
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

                # await self.send_test_request()

                await self.check_never_checked()
                await self.check_earlier_checked()

            now = datetime.datetime.now()
            total_diff = now - start
            total_time = str(total_diff).split('.')[0]
            await db.update_work_statistics(
                program_name=program_name, last_successful_at=now, duration=total_time)
            # self.logger.debug(f'[Time] {total_time}')

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
            if self.set_delete_ad_ad:
                self.logger.info(f'Deleted = {len(self.set_delete_ad_ad)}')
            if self.set_changed_ad_ad:
                self.logger.info(f'Changed = {len(self.set_changed_ad_ad)}')


async def main():
    """Проверка объявления на активность или изменение цены."""

    await db.create_db_and_tables()
    await db.start_work_statistics(program_name=program_name, started_at=start)

    obj = CheckAvBy()
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
