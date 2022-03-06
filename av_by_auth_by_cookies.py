import asyncio
import copy
import datetime
import gc
import json
import random
from html import unescape
from pathlib import Path
from typing import Optional, Any

import aiohttp
from async_timeout import timeout
from lxml.html import document_fromstring
from yarl import URL

from carprise_ftp import init_ftp
from common.errors import (
    DetailInfoError,
    DataBaseError,
    AuthError,
    ParserError,
    Error502,
    ReloadSession,
    EmptyInitialCookiesError
)
from common.log import create_logger
from common.settings import CONNECT_TO_REMOTELY_DB
from common.utils.ftp_api import Ftp404ParserError, FtpClientApiError
from models import init_db

logger = create_logger(__file__)

start = datetime.datetime.now()
program_name = Path(__file__).stem
db = init_db()
ftp = init_ftp()

TRANSLATE_EQUIPMENT = {
    "abs": "ABS",
    "alarm": "сигнализация",
    "alloy_wheels": "легкосплавные диски",
    "anti_slip_system": "антипробуксовочная ",
    "autonomous_heater": "автономный отопитель",
    "aux_ipod": "AUX или iPod",
    "bluetooth": "Bluetooth",
    "cd_mp3_player": "CD или MP3",
    "climate_control": "климат-контроль",
    "conditioner": "кондиционер",
    "cruise_control": "круиз-контроль",
    "drive_auto_start": "автозапуск двигателя",
    "electro_seat_adjustment": "электрорегулировка сидений",
    "esp": "ESP",
    "fog_lights": "противотуманные",
    "front_glass_heating": "лобового стекла",
    "front_glass_lift": "передние электро-стеклоподъёмники",
    "front_safebags": "передние",
    "hatch": "люк",
    "hitch": "фаркоп",
    "immobilizer": "иммобилайзер",
    "interior_color": "custom",
    "interior_material": "custom",
    "led_lights": "светодиодные",
    "media_screen": "мультимедийный экран",
    "mirror_dead_zone_control": "контроль мертвых зон на зеркалах",
    "mirror_heating": "зеркал",
    "navigator": "штатная навигация",
    "panoramic_roof": "панорамная крыша",
    "parktronics": "парктроники",
    "railings": "рейлинги на крыше",
    "rain_detector": "датчик дождя",
    "rear_glass_lift": "задние электро-стеклоподъёмники",
    "rear_safebags": "задние",
    "rear_view_camera": "камера заднего вида",
    "seat_heating": "сидений",
    "side_safebags": "боковые",
    "steering_wheel_heating": "руля",
    "steering_wheel_media_control": "управление мультимедиа с руля",
    "usb": "USB",
    "xenon_lights": "ксеноновые"
}

EQUIPMENT_GROUPS = {
    "abs": "Системы безопасности",
    "hatch": "Интерьер",
    "front_glass_heating": "Обогрев",
    "rain_detector": "Системы помощи",
    "interior_color": "Интерьер",
    "rear_glass_lift": "Комфорт",
    "rear_view_camera": "Системы помощи",
    "alloy_wheels": "Экстерьер",
    "interior_material": "Интерьер",
    "fog_lights": "Фары",
    "bluetooth": "Мультимедиа",
    "navigator": "Мультимедиа",
    "electro_seat_adjustment": "Комфорт",
    "front_glass_lift": "Комфорт",
    "steering_wheel_media_control": "Комфорт",
    "railings": "Экстерьер",
    "xenon_lights": "Фары",
    "parktronics": "Системы помощи",
    "drive_auto_start": "Комфорт",
    "cruise_control": "Комфорт",
    "immobilizer": "Системы безопасности",
    "aux_ipod": "Мультимедиа",
    "mirror_heating": "Обогрев",
    "usb": "Мультимедиа",
    "mirror_dead_zone_control": "Системы помощи",
    "esp": "Системы безопасности",
    "climate_control": "Климат",
    "alarm": "Системы безопасности",
    "media_screen": "Мультимедиа",
    "autonomous_heater": "Обогрев",
    "cd_mp3_player": "Мультимедиа",
    "steering_wheel_heating": "Обогрев",
    "rear_safebags": "Подушки",
    "seat_heating": "Обогрев",
    "side_safebags": "Подушки",
    "led_lights": "Фары",
    "front_safebags": "Подушки",
    "conditioner": "Климат",
    "anti_slip_system": "Системы безопасности",
    "panoramic_roof": "Интерьер",
    "hitch": "Экстерьер"
}


class AVBy:
    """Periodic collection of ads for the sale of cars."""

    TEST = False

    def __init__(self):
        self.num_of_attempts = 5
        self.sleep_time = 5
        self.time_out = 60

        self.logger = logger.getChild(self.__class__.__name__)
        self.loop = asyncio.get_running_loop()

        self.aiosession: Optional[aiohttp.ClientSession] = None

        self.user_login = None
        self.password = None
        self.main_url = 'https://av.by'
        self.login_url = 'https://av.by/login'
        self.raw_cookies_str: Optional[str] = None
        self.max_page_search = 35    # Максимальное количество первых страниц, которые нужно мониторить (по 25 на стр.)
        if self.TEST:
            self.max_page_search = 1
        self.frequency_of_checks_sek = 60   # sec       # как часто парсим

        self.dict_new_car = dict()
        self.images_dict = dict()
        self.update_dict = dict()
        self.update_list = []
        self.count_car_in_db: Optional[int]  = None
        # self.post_url = 'https://carprice.by/parcer_auto/index.php'   # 146.158.12.240
        self.post_url = 'https://146.158.13.28/parcer_auto/index.php'
        self.dict_phone = dict()
        self.dict_vin = dict()
        self.need_clear_ftp_data = True

        # Нужно ли загружать картинки
        self.need_upload_images = False

        self.initial_cookies_filename = 'initial_cookies'
        # self.initial_cookies_list: Optional[list] = None

        self.x_device_type: Optional[str] = 'web.desktop'
        self.x_api_key: Optional[str] = None   # '5e0542c3867430.91356478'
        self.x_user_group: Optional[str] = None     # '54eaff6d-a89e-4e8b-a869-21f1f2dc9105'

    async def fetch(self, url=None, method='get', phone=False, **kwargs):

        for i in range(self.num_of_attempts):
            try:
                async with timeout(self.time_out):
                    async with getattr(self.aiosession, method)(url, **kwargs) as response:
                        if response.status == 200:
                            return await response.read()
                        elif response.status == 404:
                            self.logger.info("ERROR 404 (Page Not Found): %s", url)
                            return 404
                        elif response.status == 400 and phone:
                            return response.status
                        elif response.status == 204:
                            return response.status
                        elif response.status >= 500:
                            status = response.status
                            self.logger.warning(f'[fetch] Response {status}. {url}')
                            raise Error502(f'Response {status}')
                        else:
                            self.logger.error(f"ResponseStatus {response.status}: %s", url)
                            return response.status
            except asyncio.TimeoutError:
                self.logger.debug("TIMEOUT: %s", url)
                pass

            await asyncio.sleep(random.uniform(10, self.sleep_time * 10) / 10)

    async def get_phone_number(self, ad_id, public_url):
        try:
            phone_list = []
            phone_url = f'https://api.av.by/offers/{ad_id}/phones'
            resp = await self.fetch(phone_url, phone=True)
            if isinstance(resp, int) and resp == 400:
                self.logger.warning(f"[phone] 400 Bad Request {phone_url} {public_url}")
                raise ReloadSession()
            if isinstance(resp, int) and resp == 401:
                raise AuthError('Cannot get PHONE.')
            page = resp.decode()
            resp_list_data = json.loads(page)
            for dd in resp_list_data:
                code = ''
                country = dd.get('country')
                if country:
                    code = country.get('code') or ''
                    if code:
                        code = code.strip().replace(' ', '')
                number = dd['number'].strip().replace(' ', '')
                telephone = f'+{code}{number}'.strip().replace(' ', '')
                phone_list.append(telephone)

            self.dict_phone[ad_id] = phone_list
            return phone_list

        except ReloadSession:
            raise
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def get_vin(self, ad_id) -> str:
        try:
            vin_url = f'https://api.av.by/offer-types/cars/offers/{ad_id}/vin'
            self.aiosession._default_headers['x-api-key'] = self.x_api_key
            self.aiosession._default_headers['x-device-type'] = self.x_device_type
            self.aiosession._default_headers['x-user-group'] = self.x_user_group
            resp = await self.fetch(vin_url)
            if resp == 401:
                raise AuthError('Cannot get VIN.')
            page = resp.decode()
            resp_list_data = json.loads(page)
            vin = resp_list_data['vin']
            self.dict_vin[ad_id] = vin
            return vin
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def get_new_cars_data(self):
        """Ищем только новые объявления на нескольких первых страницах или те, у которых изменилась цена.

        Для визуального просмотра переходим на https://cars.av.by/filter?sort=4&page=1
        Для запросов к апи https://api.av.by/offer-types/cars/filters/main/init?sort=4&page=1
        """

        try:
            self.dict_new_car = dict()
            self.update_dict = dict()
            self.update_list = []
            self.images_dict = dict()
            self.raised_list = []  # список поднятых объявлений

            db_equipment, db_equipment_group = await db.get_equipment()
            TRANSLATE_EQUIPMENT.update(db_equipment)
            EQUIPMENT_GROUPS.update(db_equipment_group)
            set_exist_equipment = set(list(TRANSLATE_EQUIPMENT.keys()))
            set_new_equipment = set()
            # print(json.dumps(TRANSLATE_EQUIPMENT, indent=4, ensure_ascii=False))
            # print(json.dumps(EQUIPMENT_GROUPS, indent=4, ensure_ascii=False))

            api_filter_url = 'https://api.av.by/offer-types/cars/filters/main/init'
            for num_page in range(1, self.max_page_search + 1):
                params= {
                    'sort': 4,
                    'page': num_page,
                }
                while True:
                    resp = await self.fetch(api_filter_url, params=params)
                    if resp and isinstance(resp, bytes):
                        break
                    elif resp is None:
                        await asyncio.sleep(5)
                    else:
                        raise ParserError(f"[linc] Server response.status = {resp}")
                # print(resp)
                page = resp.decode()
                cars_dict_data = json.loads(page)

                # total_cars = cars_dict_data['count']
                total_pages = cars_dict_data['pageCount']
                # current_pages = cars_dict_data['page']
                adverts = cars_dict_data['adverts']

                # self.write_to_file(file_name='cars_dict_data', data=cars_dict_data, indent=2)
                self.write_to_file(file_name='av_by', data=adverts, indent=2)

                self.logger.debug(f"[data] {num_page} - page, total_pages = {total_pages}, adverts = {len(adverts)}")
                for ad in adverts:
                    # Чтоб можно было посмотреть объявление, на котором упал парсер
                    self.write_to_file('av_by_one_car', ad)
                    is_new_car = False
                    ad_id = str(ad['id'])
                    is_exist_in_db = await db.get_exist_car(ad_id)
                    if not is_exist_in_db:
                        is_new_car = True

                    # altImage = ad['metaInfo']['altImage']
                    # name_list = altImage.split(',')[:-1]
                    # name = ','.join(name_list)
                    # title = name.strip()

                    # version = ad['version']
                    seller_name = ad['sellerName']
                    published_at = ad['publishedAt']
                    published_timestamp = datetime.datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%S%z').timestamp()
                    published_data = str(datetime.datetime.fromtimestamp(published_timestamp).date())
                    refreshed_at = ad['refreshedAt']
                    location_name = ad['locationName']
                    short_location_name = ad['shortLocationName']
                    photos_list = ad['photos']
                    all_images_list = []
                    aux_img_list = []
                    for pht in photos_list:
                        photo_url = pht['big']['url']
                        if pht['main']:
                            all_images_list.append(photo_url)
                        else:
                            aux_img_list.append(photo_url)
                    all_images_list.extend(aux_img_list)

                    metadata = ad['metadata']
                    vin = ''
                    vin_checked = False
                    vin_info = metadata.get('vinInfo')
                    if vin_info is not None:
                        vin = vin_info['vin']
                        vin_checked = vin_info['checked']
                        # list_vins.append(ad_id)

                    # year = str(metadata['year'])
                    year = str(ad['year'])

                    condition = metadata['condition']['label']
                    price = ad['price']
                    price_byn = str(price['byn']['amount'])
                    price_usd = str(price['usd']['amount'])

                    description = ad.get('description') or ''
                    description = description.replace('\n', ' ').replace('\r', '')
                    public_url = ad['publicUrl']

                    properties = ad['properties']
                    brand = ''
                    model = ''
                    engine_capacity = ''
                    engine_type = ''
                    transmission_type = ''
                    body_type = ''
                    drive_type = ''
                    color = ''
                    mileage_km = ''
                    number_of_seats = ''
                    new_equipment = False
                    engine_endurance = None
                    generation = ''
                    generation_with_years = ''
                    list_exist_keys = [
                        'brand',
                        'model',
                        'engine_capacity',
                        'engine_type',
                        'transmission_type',
                        'body_type',
                        'drive_type',
                        'color',
                        'mileage_km',
                        'condition',
                        'year',
                        'generation',               # поколение
                        'generation_with_years',
                        'number_of_seats',
                        'engine_endurance',
                    ]

                    equipment_list = []
                    for prop in properties:
                        name = prop['name'].strip()
                        value = prop['value']
                        if name == 'brand':
                            brand = value
                        elif name == 'model':
                            model = value
                        elif name == 'engine_capacity':
                            engine_capacity = value
                        elif name == 'engine_type':
                            engine_type = value
                        elif name == 'transmission_type':
                            transmission_type = value
                        elif name == 'body_type':
                            body_type = value
                        elif name == 'drive_type':
                            drive_type = value
                        elif name == 'color':
                            color = value
                        elif name == 'mileage_km':
                            mileage_km = str(value)
                        elif name == 'number_of_seats':
                            number_of_seats = value
                        elif name == 'generation':
                            generation = value
                        elif name == 'generation_with_years':
                            generation_with_years = value
                        else:
                            if name not in set_exist_equipment and name not in list_exist_keys:
                                set_new_equipment.add(name)
                                new_equipment = name
                                self.logger.warning(f'new_equipment = <{new_equipment}>, value = <{value}>')
                            if name not in list_exist_keys:
                                ru_name = TRANSLATE_EQUIPMENT.get(name)
                                if ru_name == 'custom':
                                    ru_name = value
                                equipment_list.append(ru_name)

                    title_list = []
                    title = ''
                    if brand:
                        title_list.append(brand)
                    if model:
                        title_list.append(model)
                    if generation:
                        title_list.append(generation)
                    if title_list:
                        title = ' '.join(title_list)
                    if not title:
                        raise ParserError(f'Отсутствует TITLE для {public_url}')

                    dict_data = {
                        'Car_name': title,                                  # str
                        'Phone': '',                                        # text
                        'Price_rub': price_byn,                             # str
                        'Price_dollar': price_usd,                          # str
                        'Year': year,                                       # str
                        'Vin': vin,                                         # str
                        'Сar_mileage': mileage_km,                          # int
                        'Fuel_type': engine_type,                           # str
                        'Volume': engine_capacity,                          # str
                        'Colour': color,                                    # str
                        'Car_Type': body_type,                              # str
                        'Transmission': transmission_type,                  # str
                        'Drive_Unit': drive_type,                           # str
                        'Photos': '',                                       # txt
                        'Site_link': public_url,                            # str
                        'Site_number': ad_id,                               # str
                        'Publish_date': published_data,                     # str
                        'Owner_name': seller_name,                          # str
                        'Owner_city': location_name,                        # str
                        'Description': description,                         # text
                        'Auto_info': equipment_list,                        # text
                        'Brand': brand,                                     # str
                        'Model': model,                                     # str

                        'Vin_Checked': vin_checked,                         # bool
                        'Condition_car': condition,                         # str
                        'Number_of_seats': number_of_seats,                 # str
                        'ShortLocation': short_location_name,               # str
                        'Generation': generation,                           # str
                        'Generation_with_years': generation_with_years,     # str
                        'Refreshed_at': refreshed_at,           # datetime
                        'ad_was_raised': None,
                    }

                    if new_equipment:
                        self.logger.warning(f"new_equipment_url = {dict_data['Site_link']}")

                    if is_new_car:
                        # eсли машины нет в БД
                        if phone_list := self.dict_phone.get(ad_id):
                            # Если перезагружали сессию из-за сбоя, то телефон уже есть (сохранился)
                            dict_data['Phone'] = phone_list
                        else:
                            # парсим телефон
                            phone_list = await self.get_phone_number(ad_id, public_url)
                            dict_data['Phone'] = phone_list

                        # парсим вин если есть
                        if vin:
                            # Если перезагружали сессию из-за сбоя, то VIN уже есть (сохранился)
                            if full_vin := self.dict_vin.get(ad_id):
                                dict_data['Vin'] = full_vin
                            else:
                                full_vin = await self.get_vin(ad_id)
                                dict_data['Vin'] = full_vin

                        # Отправляем картинки на загрузку
                        self.images_dict[ad_id] = all_images_list

                        self.dict_new_car[ad_id] = dict_data

                    else:
                        db_price_byn = is_exist_in_db[0]    # (Price_rub, Vin, Phone, Photos)

                        phone_db = is_exist_in_db[2]
                        phone_db = json.loads(phone_db.replace("'", '"'))
                        photos = is_exist_in_db[3]

                        dict_data['Vin'] = is_exist_in_db[1]
                        dict_data['Phone'] = phone_db
                        dict_data['Photos'] = photos

                        # Проверяем на изменение цены
                        if db_price_byn != price_byn:
                            # говорим что нужно обновить цены БД
                            self.update_list.append((price_byn, price_usd, ad_id))
                            # говорим что нужно отправить на carprice.by
                            self.update_dict[ad_id] = dict_data

                        if self.need_upload_images:
                            if photos == '[]' and all_images_list:
                                # Отправляем картинки на загрузку
                                self.images_dict[ad_id] = all_images_list
                                # говорим что нужно отправить на carprice.by
                                self.update_dict[ad_id] = dict_data

                                self.logger.debug(
                                    f'[data] There are {len(all_images_list)} new images for {public_url}')

                        # Если цена не поменялась или не появились новые картинки,
                        # то проверяем была ли просто изменена дата публикации
                        if not self.update_dict.get(ad_id):
                            last_sent_at = await db.get_last_sent_at(table_name='cars', ad_id=ad_id)
                            if last_sent_at:
                                now = datetime.datetime.now()
                                diff = now - last_sent_at
                                days = diff.days
                                if days >= 89:
                                    now_str = str(now).split('.')[0]
                                    dict_data['ad_was_raised'] = now_str
                                    # говорим что нужно отправить на carprice.by
                                    self.update_dict[ad_id] = dict_data
                                    # отмечаем в БД что объявление поднялось
                                    await db.set_ad_as_raised('cars', ad_id, now_str)
                                    self.raised_list.append(ad_id)

            if self.dict_new_car:
                self.logger.debug(f'[data] New = {len(self.dict_new_car)}')

            if self.update_dict:
                self.logger.debug(f'[data] Change = {len(self.update_dict)}')

            if self.raised_list:
                self.logger.debug(f'[data] Raised = {len(self.raised_list)}')

            if set_new_equipment:
                list_equipment = []
                for eq in set_new_equipment:
                    data = {'en_name': eq}
                    list_equipment.append(data)
                await db.add_all_value(table_name='equipment', list_dict_key_val=list_equipment)
        except ReloadSession:
            raise
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    @staticmethod
    def write_to_file(file_name: str, data: Any, mode: str = 'w+', indent: int = 4) -> None:
        with open(f'{file_name}.json', f'{mode}') as f:
            data = json.dumps(data, indent=indent, ensure_ascii=False)
            f.write(data)
            f.write('\n')

    @staticmethod
    def read_from_json_file(file_name: str) -> Any:
        with open(f'{file_name}.json', 'r') as f:
            result_str = f.read()
            result = json.loads(result_str)
        return result

    def create_file_initial_cookies(self):
        self.write_to_file(self.initial_cookies_filename, [])

    def read_initial_cookies_from_file(self, file_name: Optional[str] = None) -> list:
        initial_cookies = []
        try:
            if not file_name:
                file_name = self.initial_cookies_filename   # = 'initial_cookies'
            initial_cookies = self.read_from_json_file(file_name=file_name)
            assert isinstance(initial_cookies, list)
        except FileNotFoundError as e:
            self.logger.error(DetailInfoError(e))
            self.create_file_initial_cookies()
        except Exception as e:
            self.logger.error(DetailInfoError(e))
        return initial_cookies

    async def upload_images(self):
        """Скачивание картинок, установка водяного знака и заливка на сервер."""
        try:
            list_dict_img = []  # список, который нужен для скачивания и загрузки картинок
            set_404_path_image = set()  # список path_image с 404 ошибкой
            dict_image_path = dict()  # предварительный словарик путей, которые пойдут в POST
            total_images = 0
            transmitted_path_images = set()  # переданные для скачивания и заливки пути картинок

            for ad_id, list_images_urls in self.images_dict.items():
                list_path_img = []  # список, который пойдёт в окончательный POST запрос
                for image_url in list_images_urls:
                    total_images += 1
                    if 'advertbig' in image_url:
                        img_name = image_url.split('advertbig/')[1].replace('/','_')
                    else:
                        img_name = image_url.split('/')[-1]

                    # # полный путь, куда заливать картинки
                    # full_ftp_path_image = f'{config.image.upload_to}/{img_name}'

                    # путь, по которому картинка будет доступна на сайте
                    site_path_image = f'http://carprice.by/upload/parser_photos/{img_name}'

                    dict_data = {
                        'image_url': image_url,
                        'path_image': img_name,
                    }

                    list_path_img.append(site_path_image)

                    if ftp.success_download_path_image:
                        # Заливаем картинки только если они не были уже залиты
                        if img_name not in ftp.success_download_path_image:
                            # Заливаем, если картинки вообще есть
                            if img_name not in ftp.set_404_path_image:
                                # список картинок, которые нужно залить
                                list_dict_img.append(dict_data)
                                transmitted_path_images.add(img_name)
                    else:
                        # список картинок, которые нужно залить
                        list_dict_img.append(dict_data)
                        transmitted_path_images.add(img_name)
                # предварительный словарик путей, которые пойдут в POST
                dict_image_path[ad_id] = list_path_img

            # Скачиваем и заливаем картинки
            if list_dict_img:
                try:
                    # await ftp.download_many_img(
                    #     list_dict_url_path_img=list_dict_img,
                    #     set_watermark=True,
                    #     need_clear_data=self.need_clear_ftp_data
                    # )
                    await ftp.upload_many_img(
                        list_dict_url_path_img=list_dict_img,
                        set_watermark=True,
                        need_clear_data=self.need_clear_ftp_data
                    )
                except Ftp404ParserError:
                    set_404_path_image = ftp.set_404_path_image
                except FtpClientApiError as e:
                    if 'Temporary failure in name resolution' in str(e):
                        self.need_clear_ftp_data = False
                        raise ReloadSession()

            if set_404_path_image:
                self.logger.debug(f'[upload] 404_image_url = {len(set_404_path_image)}')

            success_download_path_image = ftp.success_download_path_image
            self.logger.debug(
                f'[upload] success download image {len(success_download_path_image)} of {total_images}')

            all_processed_img = success_download_path_image.union(set_404_path_image)
            unprocessed_images = transmitted_path_images.difference(all_processed_img)
            if unprocessed_images:
                self.logger.warning(f'[upload] Failed to download = {len(unprocessed_images)} images')
                self.need_clear_ftp_data = False
                raise ReloadSession()

            # Добавляем пути в окончательный POST запрос
            update_images_list = []
            for ad_id, list_path_images in dict_image_path.items():
                clear_photo_list = []
                if success_download_path_image:
                    for site_path_image in list_path_images:
                        path_img = site_path_image.split('/')[-1]
                        if path_img in success_download_path_image:
                            clear_photo_list.append(site_path_image)
                clear_photo_list = str(clear_photo_list)
                if self.update_dict.get(ad_id) is not None:
                    self.update_dict[ad_id]['Photos'] = clear_photo_list
                    tuple_img_update = (clear_photo_list, ad_id)
                    update_images_list.append(tuple_img_update)
                else:
                    self.dict_new_car[ad_id]['Photos'] = clear_photo_list

            # Если появились картинки, то обновляем
            if update_images_list:
                await db.update_images(table_name='cars', update_list=update_images_list)

            self.logger.debug('[upload] image path cleaned.')

        except Error502:
            raise
        except ReloadSession:
            raise
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def fill_in_equipment_table(self):
        """Заполнение таблицы 'equipment' при первом запуске."""

        list_full_equipment_data = []
        for key, val in TRANSLATE_EQUIPMENT.items():
            dict_data = {
                'group_name': EQUIPMENT_GROUPS[key],
                'en_name': key,
                'ru_name': val
                }
            list_full_equipment_data.append(dict_data)

        await db.add_all_value(table_name='equipment', list_dict_key_val=list_full_equipment_data)

    async def send_last_result(self):
        """Для отладки. Отправка последнего результата на тестовый роут '/parcer_auto/test.php'.

        Результат пишется в файл /carprice.by/parcer_auto/test.txt
        """
        try:
            list_final_data = []
            with open('result.json', 'r') as f:
                res = f.read()
                list_final_data = json.loads(res)

            post_url = 'https://carprice.by/parcer_auto/test.php'   # 146.158.12.240
            # post_url = 'https://146.158.12.240/parcer_auto/test.php'   # 146.158.12.240
            # post_url = 'https://146.158.13.28/parcer_auto/test.php'
            self.aiosession._default_headers['Content-Type'] = 'application/x-www-form-urlencoded'
            # self.aiosession._default_headers['Content-Type'] = 'multipart/form-data'
            # self.aiosession._default_headers['Content-Type'] = 'application/json'

            # https://cars.av.by/mitsubishi/lancer/100445344
            post_data1 = {
                "Car_name": "Mitsubishi Lancer X",
                "Phone": [
                    "+375447568108"
                ],
                "Price_rub": "13184",
                "Price_dollar": "5199",
                "Year": "2008",
                "Vin": "JMBSRCY2A9U004783",
                "Сar_mileage": "198000",
                "Fuel_type": "бензин",
                "Volume": "1.5",
                "Colour": "чёрный",
                "Car_Type": "седан",
                "Transmission": "автомат",
                "Drive_Unit": "передний привод",
                "Photos": "",
                "Site_link": "https://cars.av.by/mitsubishi/lancer/100445344",
                "Site_number": "100445344",
                "Publish_date": "2021-06-22",
                "Owner_name": "Вадим ",
                "Owner_city": "Минск",
                "Description": "Классический автомат, переключает плавно, без пинков и рывков. Заменены диски, колодки, масло, фильтр, сделан ручник, схождение. Ошибок нет. +комплект зимы на литье. Автохаусам просьба не беспокоить. ",
                "Auto_info": [
                    "легкосплавные диски",
                    "ABS",
                    "ESP",
                    "антипробуксовочная ",
                    "сигнализация",
                    "передние",
                    "парктроники",
                    "тёмный",
                    "ткань",
                    "автозапуск двигателя",
                    "передние электро-стеклоподъёмники",
                    "задние электро-стеклоподъёмники",
                    "сидений",
                    "зеркал",
                    "кондиционер",
                    "AUX или iPod",
                    "CD или MP3"
                ],
                "Brand": "Mitsubishi",
                "Model": "Lancer",
                "Vin_Checked": True,
                "Condition_car": "с пробегом",
                "Number_of_seats": "",
                "ShortLocation": "Минск",
                "Generation": "X",
                "Generation_with_years": "X (2007 - 2010)",
                "Refreshed_at": "2021-07-06T07:48:19+0000",
                "ad_was_raised": None,
                'TIME': str(datetime.datetime.now()),
                'POST': 1
            }
            # resp = await self.fetch(post_url, method='post', data=post_data1)
            # print(f'1 - {resp = }')
            # print('=' * 50)

            post_data2 = {
                'Car_name': 'BMW X4 G02',
                'Phone': ['+375296071177', '+375172320000'],
                'Price_rub': '141895',
                'Price_dollar': '55120',
                'Year': '2019',
                'Vin': 'WBAVJ11080LS29724',
                'Сar_mileage': '18154',
                'Fuel_type': 'дизель',
                'Volume': '2.0',
                'Colour': 'красный',
                'Car_Type': 'внедорожник 5 дв.',
                'Transmission': 'автомат',
                'Drive_Unit': 'постоянный полный привод',
                'Photos': '',
                'Site_link': 'https://cars.av.by/bmw/x4/100468234',
                'Site_number': '100468234',
                'Publish_date': '2021-07-05',
                'Owner_name': 'АВТОИДЕЯ',
                'Owner_city': 'Минск',
                # 'Description': 'Оригинальный пробег, на гарнатии, без замечаний после комплексной диагностики. Передняя часть автомобиля оклеена защитной пленкой.  Заберем Ваш автомобиль в зачет по системе Trade-In.  Предлагаем тест-драйв перед покупкой. Продавец - автоцентр BMW АВТОИДЕЯ - официальный дилер BMW и MINI. Премиальный сервис и отношение к каждому клиенту.  Комплектация:  1AG Увеличенный топливный бак 225 Стандартные настройки подвески 248 Система обогрева рулевого колеса 258 Шины допускающие движение в аварийном режиме 28G 19\'\' легкосплавные диски Y-spoke стиль 694 с run-flat премуществами 2TE 8  ст. АКПП Steptronic с подрулевыми лепестками переключения передач 2VB Индикация давления в шинах 322 Комфортный доступ 3KA Остекление с улучшенной звукоизоляцией 430 Внутренние и наружные зеркала с затемнением 431 Внутреннее зеркало с автоматическим затемнением 441 Комплект для курящих 459 Электрорегулировка положения сиденья с функцией памяти 481 Спортивные сиденья для водителя и переднего пассажира 494 Система обогрева сиденья водителя и переднего пассажира 4AW Панель приборов Sensatec 4K1 Исполнение из ценных пород древесины "Дуб" 4UR Сопровождающее освещение салона 534 Автоматическая система кондиционирования 552 Адаптивные светодиодные фары 5AC Автоматическое включение дальнего света 5DM Ассистент парковки 650 Дисковод CD 676 HiFi акустическая система 6U3 BMW Live кокпит Professional 775 BMW Individual обивка потолка салона "Антрацит" 7HW Отделка X Line 842 Исполнение для стран с холодным климатом ',
                'Description': 'Оригинальный',
                'Auto_info': str([
                    'легкосплавные диски',
                    'ABS', 'ESP',
                    'антипробуксовочная ',
                    'иммобилайзер',
                    'передние',
                    'боковые',
                    'задние',
                    'датчик дождя',
                    'камера заднего вида',
                    'парктроники',
                    'тёмный',
                    'комбинированные материалы',
                    'круиз-контроль',
                    'управление мультимедиа с руля',
                    'электрорегулировка сидений',
                    'передние электро-стеклоподъёмники',
                    'задние электро-стеклоподъёмники',
                    'сидений',
                    'зеркал',
                    'руля',
                    'климат-контроль',
                    'кондиционер',
                    'AUX или iPod',
                    'Bluetooth',
                    'CD или MP3',
                    'USB',
                    'мультимедийный экран',
                    'штатная навигация',
                    'светодиодные'
                ]),
                'Brand': 'BMW',
                'Model': 'X4',
                'Vin_Checked': True,
                'Condition_car': 'с пробегом',
                'Number_of_seats': '',
                'ShortLocation': 'Минск',
                'Generation': 'G02',
                'Generation_with_years': 'G02 (2018 - ...)',
                'Refreshed_at': '2021-07-10T06:54:38+0000',
                'ad_was_raised': None,
                'TIME': str(datetime.datetime.now()),
                'POST': 2
            }
            description = post_data2['Description']
            description = description.replace('\n', ' ').replace('\r', '')
            post_data2['Description'] = description
            # resp = await self.fetch(post_url, method='post', data=post_data2)
            # print(f'2 - {resp = }')
            # print('-*/'*50)

            # for post_data in list_final_data:
            #     ad_id = post_data['Site_number']
            #     if ad_id != post_data2['Site_number']:
            #         continue
            #     description = post_data['Description']
            #     description = description.replace('\n', ' ').replace('\r', '')
            #     post_data['Description'] = description
            #     post_data['TIME'] = str(datetime.datetime.now())
            #     post_data['POST'] = 3
            #
            #     # if post_data != post_data2:
            #     #     for k, v in post_data.items():
            #     #         v2 = post_data2[k]
            #     #         if v != v2:
            #     #             print(f'3 - {k}: {v}')
            #     #             print(f'2 - {k}: {v2}')
            #
            #     # print(post_data)
            #     # print(json.dumps(post_data, indent=4, ensure_ascii=False))
            #
            #
            #     # post_data = json.dumps(list_final_data)
            #     resp = await self.fetch(post_url, method='post', data=post_data)
            #     # resp = await self.fetch(self.post_url, method='post', json=post_data)
            #     print(f'3 - {resp = }')
            #
            #     # break
        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def set_start_cookies_in_aiosession(self):
        """Getting cookies from headless browser using chromedriver and set them in aiosession."""

        try:
            initial_cookies = self.read_initial_cookies_from_file(file_name=self.initial_cookies_filename)
            if not initial_cookies:
                raise EmptyInitialCookiesError()

            # set cookies in aiohttp.ClientSession
            list_cookies = []
            for dict_cookie in initial_cookies:
                name = dict_cookie['name']
                value = dict_cookie['value']
                cookies = {name: value}
                cookie_str = f'{name}={value}'
                list_cookies.append(cookie_str)
                self.aiosession.cookie_jar.update_cookies(cookies, URL(self.main_url))
                if name == 'userGroup':
                    self.x_user_group = value
                if name == 'X-Api-Key':
                    self.x_api_key = value

            if not self.x_user_group:
                raise ParserError('Empty self.x_user_group')
            if not self.x_api_key:
                raise ParserError('Empty self.x_api_key')

            self.logger.debug(f'{self.x_api_key = }')
            self.logger.debug(f'{self.x_user_group = }')

            # set cookies in headers
            self.start_cookies_str = '; '.join(list_cookies)
            self.aiosession._default_headers['Cookie'] = self.start_cookies_str

        except Exception as e:
            self.logger.error(DetailInfoError(e))
            raise

    async def check_auth(self):
        resp = await self.fetch(self.main_url)
        page = unescape(resp.decode('windows-1251'))
        tree = document_fromstring(page)
        user_name = tree.find('.//h4[@class="nav__dropdown-name"]/span').text_content().strip()

        if user_name != 'Артем':
            raise AuthError("[check_auth] Can't find user_name in main page.")

        self.logger.debug('[auth] Authorization was successful.')

    async def run(self):
        """Entry point."""

        try:
            if self.aiosession:
                await self.aiosession.close()
                self.aiosession = None

            # заполняем таблицу 'equipment' при первом запуске
            if not await db.count_elem('equipment'):
                await self.fill_in_equipment_table()

            user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                         'Chrome/88.0.4324.150 Safari/537.36'
            headers = {
                'User-Agent': user_agent,
            }
            conn = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=conn, headers=headers) as self.aiosession:

                await self.set_start_cookies_in_aiosession()
                await self.check_auth()

                while True:
                    gc.collect()
                    begin = datetime.datetime.now()
                    await self.get_new_cars_data()
                    gc.collect()

                    # TODO: Включить проверку ftp соединения в main()
                    # Скачиваем и заливаем картинки если нужно
                    if self.need_upload_images:
                        if self.images_dict:
                            count_img = 0
                            for list_img in self.images_dict.values():
                                count_img += len(list_img)
                            self.logger.debug(f"[img] Need download {len(self.images_dict)} ad, {count_img} images.")
                            try:
                                await self.upload_images()
                            except Error502 as e:
                                # отвалился сервер с картинками спим 2 мин
                                self.logger.warning(DetailInfoError(e, 'FTP Response 502'))
                                await asyncio.sleep(120)
                                continue

                    gc.collect()

                    list_new_car = [item for item in self.dict_new_car.values()]
                    list_changed_car = []
                    if self.update_dict:
                        list_changed_car = [item for item in self.update_dict.values()]
                        # Обновляем цены
                        await db.update_price(self.update_list)

                    list_final_data = copy.deepcopy(list_new_car)
                    list_final_data.extend(list_changed_car)
                    data = json.dumps(list_final_data, indent=4, ensure_ascii=False)
                    with open('result.json', 'w') as f:
                        f.write(data)

                    gc.collect()

                    # В БД добавляем только новые
                    if list_new_car:
                        for dict_data in list_new_car:
                            dict_data['ad_id'] = dict_data['Site_number']
                            dict_data['Phone'] = str(dict_data['Phone'])
                            dict_data['Photos'] = str(dict_data['Photos'])
                            dict_data['Auto_info'] = str(dict_data['Auto_info'])
                        await db.add_all_value(table_name='cars', list_dict_key_val=list_new_car)
                        self.logger.debug('[save] successfully saved in database.')

                    await ftp.clear_data()

                    """------------------ Отправка на сайт -----------------"""
                    self.aiosession._default_headers['Content-Type'] = 'application/x-www-form-urlencoded'
                    if list_final_data:
                        total_ad = len(list_final_data)
                        count_send = 0
                        for post_data in list_final_data:
                            post_data['Auto_info2[]'] = post_data['Auto_info']
                            self.write_to_file('Try_post_av_by', post_data)
                            ad_id = post_data['Site_number']
                            site_link = post_data['Site_link']
                            while True:
                                try:
                                    if self.TEST:
                                        resp = '\n\r'
                                    else:
                                        resp = await self.fetch(self.post_url, method='post', data=post_data)
                                    # resp = '\n\r'
                                except Error502 as e:
                                    error_message = str(e)
                                    self.logger.error(f'[post] {error_message} Can not send {ad_id} {site_link}')
                                    break
                                if resp is not None and not isinstance(resp, int):
                                    break
                                elif isinstance(resp, int):
                                    self.logger.error(f'[post] Response = {resp} Can not send {ad_id} {site_link}')
                                    break
                                else:
                                    self.logger.warning(f'[post] Trying to send again {ad_id} {site_link}')

                            if not self.TEST:
                                count_send += 1
                                self.logger.debug(f'[post] {count_send} из {total_ad}')

                            # Отмечаем, что объявление отправлено
                            post_data_str = json.dumps(post_data, indent=2, ensure_ascii=False)
                            await db.mark_ad_as_sent('cars', post_data_str, ad_id)
                            # self.logger.debug(f'[post] {ad_id}')

                        if not self.TEST:
                            self.logger.debug(f'[post] successfully to {self.post_url}.')

                    """-------------------------------------------------------"""

                    now = datetime.datetime.now()
                    diff = now - begin
                    total_diff = now - start
                    end = str(diff).split('.')[0]
                    total_time = str(total_diff).split('.')[0]
                    await db.update_work_statistics(
                        program_name=program_name, last_successful_at=now, duration=total_time)
                    self.logger.debug(
                        f'[Time] \033[32m{end}\033[0m. Total duration: \033[34m{total_time}\033[0m. Sleep ........')

                    if self.TEST:
                        break

                    if diff.seconds > self.frequency_of_checks_sek:
                        continue
                        # break
                    await asyncio.sleep(self.frequency_of_checks_sek)

                    self.dict_phone = dict()
                    self.dict_vin = dict()
                    self.need_clear_ftp_data = True
                    # break

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

    if CONNECT_TO_REMOTELY_DB:
        if await db.test_connect_db():
            logger.debug(f"[MySQL] Connection to the '{db.db}' database is successful !!!")
        else:
            raise DataBaseError(f"Can't connect to the '{db.db}' remotely database.")
    # await ftp.check_ftp_config()
    await db.create_db_and_tables()

    await db.start_work_statistics(program_name=program_name, started_at=start)

    av_by = AVBy()
    await av_by.run()

    # now = datetime.datetime.utcnow()
    # cookies_list = av_by.read_initial_cookies_from_file()
    # # cookies_list = av_by.read_initial_cookies_from_file(file_name='initial_cookies_default')
    # count_name = 0
    # for cookie in cookies_list:
    #     name = cookie['name']
    #     count_name += 1
    #     expiry = cookie.get('expiry')
    #     if expiry:
    #         dt_expiry = datetime.datetime.fromtimestamp(expiry)
    #         diff = dt_expiry - now
    #         diff_day = diff.days
    #         print(diff_day,  dt_expiry, name)
    # print(f'{count_name = }')


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
