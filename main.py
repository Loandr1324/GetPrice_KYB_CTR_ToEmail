# Author Loik Andrey 7034@balancedv.ru
import io
import config
import pandas as pd
from ftplib import FTP
import smbclient
from loguru import logger
import send_mail
from datetime import datetime

logger.add(config.FILE_NAME_CONFIG,
           format="{time:DD/MM/YY HH:mm:ss} - {file} - {level} - {message}",
           level="INFO",
           rotation="1 month",
           compression="zip")


def get_stock_ftp(file: str) -> pd.DataFrame:
    """Получаем файл с остатками по FTP

    :return DataFrame с остатками"""
    host = config.FTP_AUTH['HOST']
    user = config.FTP_AUTH['USER']
    psw = config.FTP_AUTH['PSW']

    with FTP(host) as ftp:
        ftp.login(user=user, passwd=psw)
        ftp.encoding = 'utf-8'

        logger.info(f"Read file '{file}' via ftp ")

        r = io.BytesIO()
        ftp.retrbinary('RETR ' + file, r.write, 1024)
        df = pd.read_csv(io.BytesIO(r.getvalue()), delimiter=';')
        df = df.drop(columns=['price', 'currency'])
    return df


def get_price():
    """
    Считываем файлы с ценами из папки в локальной сети

    :return: DataFrame с ценами
    """
    # Получаем список файлов на сервере
    path = config.LOCAL_PATH['PATH']
    smbclient.ClientConfig(username=config.LOCAL_PATH['USER'], password=config.LOCAL_PATH['PSW'])
    list_file = smbclient.listdir(path)

    # Считываем все файлы с ценами KYB и CTR в DataFrame
    df = pd.DataFrame()
    for item in list_file:
        if item.endswith('.xlsx') and (item.find('Цены KYB') != -1 or item.find('Цены CTR') != -1):

            logger.info(f"Read file '{item}' from local server")

            path_file = path + "\\" + item
            with smbclient.open_file(path_file, mode="rb") as fd:
                file_bytes = fd.read()
                df_price = pd.read_excel(file_bytes)
            df = pd.concat([df, df_price], axis=0, ignore_index=True)
            df = df[['articul', 'brand', 'price']]
    return df


def send_df_to_email(file: str, file_temp: str):
    """
    Отправляем на почту получателей временный файл file_temp('temp.xlsx' или 'temp1.xlsx') под именем file.
    Адреса получателя зависят от имени временного файла.
    Адреса получателя берутся из файла config из словаря TO_EMAILS.

    :param file: -> str. Наименование файла с расширением .csv, который будет подставляться в письме.
    :param file_temp: -> str. Наименование временного файла с данными.
    :return: None
    """
    # Изменяем расширение в наименовании файла
    filename = file[:-3] + 'xlsx'
    logger.info(f"We send a temporary file '{file_temp}' under the name file '{filename}'")

    # Создаём словарь с данными, для письма и передаём в функцию для отправки письма
    if file_temp == 'temp.xlsx':
        message = {
            'Subject': f"Прайс-лист {filename}",
            'email_content': f"Сгенерирован прайс лист: {filename}",
            'To': config.TO_EMAILS['TO_CORRECT'],
            'File_name': filename,
            'Temp_file': file_temp
            }
        send_mail.send(message)

    # Создаём словарь с данными, для письма и передаём в функцию для отправки письма
    elif file_temp == 'temp1.xlsx':
        message = {
            'Subject': f"Прайс-лист без цен {filename}",
            'email_content': f"Не были найдены цены на товары со склада: {filename}",
            'To': config.TO_EMAILS['TO_ERROR'],
            'File_name': filename,
            'Temp_file': file_temp
            }
        send_mail.send(message)
    return


def get_prices_to_email():
    """ Объединяем файл остатков по каждому городу и файл с ценами. Отправляем полученный файл на почту """
    # Создаём DataFrame для ошибок
    df_error = pd.DataFrame()

    # Считываем прайсы с локального сервера
    df_price = get_price()

    # Подставляем цены для остатков по каждому складу и отправляем на почту
    file_list_stock = config.FILE_LIST_STOCK
    for file in file_list_stock:
        df_stock = get_stock_ftp(file)
        df_result = pd.merge(df_stock, df_price, on=['articul', 'brand'], how='left')

        # Сохраняем корректные данные файл для дальнейшей отправке по почте
        df_correct = df_result[~df_result['price'].isna()]
        df_correct.to_excel('temp.xlsx', index=False)

        # Отправляем на почту файл с корректными данными
        if len(df_correct) > 0:
            send_df_to_email(file, 'temp.xlsx')

        # Добавляем данные в DataFrame с ошибками
        df_isnan = df_result[df_result['price'].isna()]
        df_error = pd.concat([df_error, df_isnan], axis=0, ignore_index=True)

    # Записываем файл с ошибками и отправляем на почту.
    # Запускаем эту часть кода, только в Понедельник между 01 и 02 часами по UTC.
    day_hour_now = datetime.utcnow().strftime('%a %H')
    if day_hour_now == 'Mon 01':
        logger.info(f"Day weekly and hour: '{day_hour_now}' send to email's error file")

        df_error = df_error[['articul', 'brand']].drop_duplicates()
        df_error.to_excel('temp1.xlsx', index=False)
        if len(df_error) > 0:
            send_df_to_email('error.csv', 'temp1.xlsx')
    else:
        logger.info(f"Day weekly and hour: '{day_hour_now}' don't send to email's error file")
    return


def main():
    get_prices_to_email()


if __name__ == '__main__':
    main()


