from PIL import Image

from common.config import config
from common.settings import BASE_DIR
from common.utils.ftp_api import FtpClientApi


class CarPriceFtpClientApi(FtpClientApi):
    def __init__(self):
        super(CarPriceFtpClientApi, self).__init__()

    async def set_watermark(self, image: Image.Image, name_img: str, *args, **kwargs) -> Image.Image:
        watermark_path = BASE_DIR / config.image.watermark.file_name
        # delete_watermark = config.image.watermark.delete_watermark
        # print('watermark_path =', watermark_path)
        # print('delete_watermark =', delete_watermark)
        # print('name_img =', name_img)

        watermark = Image.open(watermark_path)
        width = image.width
        height = image.height
        poz1 = int((width / 2) - 415)
        poz2 = int((height / 2) - 415)

        # вставка водяного знака
        image.paste(watermark, (poz1, poz2),  watermark)

        return image


def init_ftp():
    return CarPriceFtpClientApi()
