import io
import config
import pandas as pd
from ftplib import FTP
import smbclient
import logging

logging.basicConfig(filename="KYB_CTR_to_abcp.log", level=logging.INFO)


def get_stock_ftp(file: str) -> pd.DataFrame:
    """Получаем файл с остатками по FTP

    :return DataFrame с остатками"""
    host = config.FTP_AUTH['HOST']
    user = config.FTP_AUTH['USER']
    psw = config.FTP_AUTH['PSW']

    with FTP(host) as ftp:
        ftp.login(user=user, passwd=psw)
        ftp.encoding = 'utf-8'
        print(file)
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
            print(item) # TODO Удалить после тестов
            path_file = path + "\\" + item
            df_price = pd.read_excel(path_file)
            df = pd.concat([df, df_price], axis=0, ignore_index=True)
            df = df[['articul', 'brand', 'price']]
    # df.to_excel('test.xlsx') # TODO  Удалить после тестов
    return df


def create_output_files(df):
    print(df)
    # print(df['price'].isna())
    df_correct = df[~df['price'].isna()]
    df_isnan = df[~df['price'].isna()]
    print(df_correct)
    pass


def send_mail():
    """ Отправляем файл на почту"""
    pass


def get_prices_to_email():
    """ Объединяем файлы остатков по каждому городу и цен. Отправляем полученный файл на почту """
    # Считываем прайсы
    df_price = get_price()

    # Подставляем цены для каждого склада и отправляем на почту
    file_list_stock = config.FILE_LIST_STOCK
    for file in file_list_stock:
        df_stock = get_stock_ftp(file)
        df_result = pd.merge(df_stock, df_price, on=['articul', 'brand'], how='left')
        create_output_files(df_result)
        df_result.to_excel('test1.xlsx')
        break

    send_mail()
    pass


def main():
    get_prices_to_email()


if __name__ == '__main__':
    main()


