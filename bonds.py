import os
import pandas as pd
from datetime import date, timedelta
import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
from issuers import ISSUERS_DICT
from issues import ISSUES_DICT
from dotenv import load_dotenv


load_dotenv()
LOGIN = os.getenv('LOGIN')
PASSWORD = os.getenv('PASSWORD')
BASE_URL = 'https://rusbonds.ru'

# Функции для работы с данными
def parse_table(html: str) -> list:
    """Парсинг таблицы с данными облигаций"""
    soup = bs(html, 'html.parser')
    rows = soup.select('tbody tr.el-table__row')
    
    columns_config = {
        0: ('div div span div a span', 'name'),
        1: ('div div span div span', 'isin'),
        2: ('div div span div span', 'nac'),
        3: ('div div span div a span', 'issuer'),
        4: ('div div span div span', 'duration'),
        5: ('div div span div span', 'yield_rate'),
        6: ('div div span div span', 'price'),
        7: ('div div span div span', 'outstanding_volume'),
        8: ('div div span div span', 'amount_of_deals'),
        9: ('div div span div span', 'trading_volume'),
        10: ('div div span div span', 'coupon_rate'),
    }
    
    data = []
    for row in rows:
        cells = row.select('td.el-table__cell')[2:]
        item = {}
        for idx, (selector, key) in columns_config.items():
            if idx < len(cells):
                element = cells[idx].select_one(selector)
                item[key] = element.text if element else None
        data.append(item)
    
    return data

def convert_numeric_value(value: str) -> float:
    """Преобразование строковых числовых значений с пробелами в float"""
    if not value or not isinstance(value, str):
        return 0.0
    return float(value.replace(' ', '').replace(',', '.'))

def improve_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Приведение столбцов к нужным типам и расчет производных показателей"""
    df = df.copy()
    
    # Преобразование числовых столбцов
    numeric_cols = ['outstanding_volume', 'trading_volume', 'amount_of_deals']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(convert_numeric_value)
    
    # Преобразование процентных и других числовых столбцов
    float_cols = ['duration', 'yield_rate', 'coupon_rate', 'price', 'nac']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Расчет производных показателей
    if 'trading_volume' in df.columns and 'outstanding_volume' in df.columns:
        df['share_of_trading_volume'] = df['trading_volume'] / df['outstanding_volume']
    
    # Преобразование процентов
    df['yield_rate'] = df['yield_rate'] / 100
    df['coupon_rate'] = df['coupon_rate'] / 100
    
    return df

# Функции для работы с датами
def format_date_moex(date_obj: date) -> str:
    """Преобразование даты в формат для MOEX ISS"""
    return date_obj.strftime("%Y-%m-%d")

def format_date_cbr(date_obj: date) -> str:
    """Преобразование даты в формат для ЦБ РФ"""
    return date_obj.strftime("%d/%m/%Y")

def fetch_moex_data(secid: str, date_from: date, date_to: date) -> pd.DataFrame:
    """Получение данных с MOEX ISS"""
    try:
        url = (f'https://iss.moex.com/iss/history/engines/stock/'
               f'markets/bonds/securities/{secid}.html'
               f'?from={format_date_moex(date_from)}&till={format_date_moex(date_to)}')
        
        response = requests.get(url, timeout=10)
        response.encoding = 'utf-8'
        
        soup = bs(response.text, 'html.parser')
        rows = soup.select('tr')
        
        if not rows:
            return pd.DataFrame()
        
        # Извлечение заголовков
        headers = [th.text for th in rows[0].select('th')]
        
        # Извлечение данных
        data = []
        for row in rows[1:]:
            cells = [td.text for td in row.select('td')]
            if cells:
                data.append(cells)
        
        df = pd.DataFrame(data, columns=headers)
        
        # Переименование и фильтрация столбцов
        df.columns = [col.split()[0] for col in df.columns]
        required_cols = ['BOARDID', 'TRADEDATE', 'SECID', 'VALUE', 'NUMTRADES']
        
        if all(col in df.columns for col in required_cols):
            df = df[required_cols]
            df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
            df['NUMTRADES'] = pd.to_numeric(df['NUMTRADES'], errors='coerce')
            return df[df['BOARDID'] == 'TQIR']
    
    except Exception as e:
        print(f'Ошибка получения данных с MOEX: {e}')
    
    return pd.DataFrame()

def get_exchange_rate(currency: str, date_obj: date) -> float:
    """Получение курса валюты от ЦБ РФ на указанную дату"""
    try:
        url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={format_date_cbr(date_obj)}'
        response = requests.get(url, timeout=10)
        xml = ET.fromstring(response.content)
        
        rate_element = xml.find(f'.//Valute[CharCode="{currency}"]/Value')
        if rate_element is not None:
            return float(rate_element.text.replace(',', '.'))
    except Exception as e:
        print(f'Ошибка получения курса валюты: {e}')
    
    return 1.0

def process_yuan_bond(df: pd.DataFrame, isin: str) -> pd.DataFrame:
    """Специальная обработка для облигаций в юанях"""
    df = df.copy()
    
    # Находим индекс облигации в юанях
    yuan_bond_idx = df[df['isin'] == isin].index
    if len(yuan_bond_idx) == 0:
        return df
    
    idx = yuan_bond_idx[0]
    
    # Даты для расчета недельных данных
    monday = date.today() - timedelta(days=7)
    friday = date.today() - timedelta(days=3)
    
    # Получаем данные с MOEX
    moex_data = fetch_moex_data(isin, monday, friday)
    
    if not moex_data.empty:
        # Рассчитываем суммарные показатели за неделю
        total_value = moex_data['VALUE'].sum()
        total_trades = moex_data['NUMTRADES'].sum()
        
        # Получаем курс юаня
        exchange_rate = get_exchange_rate('CNY', friday)
        
        # Обновляем данные в DataFrame
        if 'share_of_trading_volume' in df.columns:
            df.at[idx, 'share_of_trading_volume'] = total_value / exchange_rate / df.at[idx, 'outstanding_volume']
        
        if 'trading_volume' in df.columns:
            df.at[idx, 'trading_volume'] = total_value
        
        if 'amount_of_deals' in df.columns:
            df.at[idx, 'amount_of_deals'] = total_trades
        
        df.at[idx, 'start_week'] = monday
    
    return df

def setup_browser():
    """Настройка и возврат экземпляра браузера"""
    options = webdriver.ChromeOptions()
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--headless=new')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(5)
    return driver

def login_to_rusbonds(driver):
    """Авторизация на RUSBONDS"""
    driver.get(f'{BASE_URL}/login')
    
    # Ввод логина
    login_input = driver.find_element(By.CSS_SELECTOR, 'div.el-input input.el-input__inner')
    login_input.clear()
    login_input.send_keys(LOGIN)
    time.sleep(1)
    
    # Ввод пароля
    password_input = driver.find_element(By.XPATH, '//input[@type="password"]')
    password_input.clear()
    password_input.send_keys(PASSWORD)
    time.sleep(1)
    
    # Нажатие кнопки входа
    login_button = driver.find_element(By.XPATH, '//button[contains(@class, "el-button--primary")]')
    login_button.click()
    time.sleep(3)

def configure_table_fields(driver, checkbox_index: int):
    """Настройка отображаемых полей в таблице"""
    # Открытие меню выбора полей
    choose_fields = driver.find_element(By.CSS_SELECTOR, 'div.select-fields div.view')
    choose_fields.click()
    time.sleep(1)
    
    # Выбор нужной группы полей
    fields_group = driver.find_element(By.XPATH, '//div[contains(text(), "Основные")]')
    fields_group.click()
    time.sleep(2)
    
    # Скролл и выбор нужного чекбокса
    checkbox = driver.execute_script(f"return document.getElementsByClassName('el-checkbox')[{checkbox_index}];")
    driver.execute_script("arguments[0].scrollIntoView(true);", checkbox)
    time.sleep(1)
    driver.execute_script("arguments[0].click();", checkbox)
    time.sleep(1)
    
    # Применение изменений
    apply_button = driver.find_element(By.XPATH, '//button[contains(@class, "el-button--primary")]')
    apply_button.click()
    time.sleep(2)

def scrape_data(driver, portfolio_item_xpath: str, watchlist_xpath: str, checkbox_index: int) -> pd.DataFrame:
    """Основная функция сбора данных"""
    # Переход в портфель
    portfolio = driver.find_element(By.XPATH, '//*[@id="navbar"]/section/div/div/div[1]/nav/div[4]')
    portfolio.click()
    time.sleep(2)
    
    # Выбор типа портфеля
    portfolio_type = driver.find_element(By.XPATH, portfolio_item_xpath)
    portfolio_type.click()
    time.sleep(3)
    
    # Переход в watchlist
    watchlist = driver.find_element(By.XPATH, watchlist_xpath)
    watchlist.click()
    time.sleep(2)
    
    # Настройка полей таблицы
    configure_table_fields(driver, checkbox_index)
    
    # Сбор данных со всех страниц
    all_data = []
    wait = WebDriverWait(driver, 10)
    actions = ActionChains(driver)
    
    while True:
        # Парсинг текущей страницы
        html = driver.page_source
        page_data = parse_table(html)
        all_data.extend(page_data)
        
        # Попытка перейти на следующую страницу
        try:
            next_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.btn-next'))
            )
            actions.move_to_element(next_button).perform()
            time.sleep(1)
            next_button.click()
            time.sleep(2)
        except Exception:
            break
    
    # Преобразование в DataFrame и удаление дубликатов
    df = pd.DataFrame(all_data)
    df.drop_duplicates(keep='first', inplace=True)
    
    return df

def main():
    """Основная функция выполнения скрипта"""
    # Сбор данных по МФО
    driver = setup_browser()
    
    try:
        login_to_rusbonds(driver)
        mfo_data = scrape_data(
            driver,
            '//*[@id="__layout"]/div/div/main/div/div/div[1]/section/div/ul/li[2]',
            '//*[@id="__layout"]/div/div/main/div/div/div[2]/section/div/ul/li[2]/div/div/span',
            90
        )
    finally:
        driver.quit()
    
    # Сбор данных по Коллекторам
    driver = setup_browser()
    
    try:
        login_to_rusbonds(driver)
        collector_data = scrape_data(
            driver,
            '//*[@id="__layout"]/div/div/main/div/div/div[1]/section/div/ul/li[1]',
            '//*[@id="__layout"]/div/div/main/div/div/div[2]/section/div/ul/li[2]/a',
            74
        )
    finally:
        driver.quit()
    
    # Обработка данных МФО
    mfo_proc = improve_dataframe(mfo_data)
    mfo_proc = process_yuan_bond(mfo_proc, 'RU000A105N25')
    
    # Обработка данных Коллекторов
    collector_proc = improve_dataframe(collector_data)
    collector_proc['start_week'] = date.today() - timedelta(days=7)
    
    # Подготовка для БД
    mfo_db = mfo_proc.copy()
    collector_db = collector_proc.copy()
    
    # Добавление ID эмитентов и выпусков
    for df in [mfo_db, collector_db]:
        df['issuer_id'] = df['issuer'].map(ISSUERS_DICT)
        df['issue_id'] = df['name'].map(ISSUES_DICT)
    
    # Объединение данных
    all_data = pd.concat([mfo_db, collector_db], ignore_index=True)
    
    # Добавление флага коллектора
    collector_ids = [12, 13, 15]
    all_data['is_collector'] = all_data['issuer_id'].apply(
        lambda x: 1 if x in collector_ids else 0
    )
     
    final_data.reset_index(drop=True, inplace=True)
      
    print(f'Получено записей: {len(final_data)}')
    
    return final_data

if __name__ == '__main__':
    final_data = main()