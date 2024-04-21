from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict
import re
import os
import requests


def parse_html(html: str) -> Dict[str, Dict[str, List[str]]]:
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all('article')
    raw_data = {}
    for article in articles:
        caption = article.find('caption').text.strip()
        table = article.find('table')
        headers = [th.text.strip()
                   for th in table.find('thead').find_all('th')]
        table_data = parse_table(table)
        raw_data[caption] = {'headers': headers, 'data': table_data}
    return raw_data


def fetch_html(url: str) -> str:
    response = requests.get(url)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    return response.text


def parse_table(table) -> List:
    rows = []
    descriptor = False
    for tr in table.find('tbody').find_all('tr'):
        row = []
        for td in tr.find_all('td'):
            if td.text == 'Descriptors*':
                descriptor = True
            if td.text not in ['Descriptors*', '', '\xa0']:
                if descriptor == False:
                    row.append(td.text.strip())
                # Means we found a descriptor before, no need to check
                elif extract_number(td.text).isnumeric():
                    # If it's a number make no modifications
                    row.append(td.text.strip())
                else:
                    # Adding Descriptor_ text to the start of string for further manipulations
                    row.append(f'Descriptor_{td.text.strip()}')
        if row != []:
            rows.append(row)
    return rows


def handle_descriptors(df: pd.DataFrame, name_column: str) -> pd.DataFrame:
    df['Descriptor'] = df[name_column].apply(
        lambda x: 1 if 'Descriptor_' in x else 0)
    df[name_column] = df[name_column].apply(
        lambda x: x.replace('Descriptor_', ''))
    return df


def extract_number(value: str) -> str:
    return re.sub(r'^\D+|\D+$', '', value).replace(',', '')


def transform_data(data: List[List], headers: List[str]) -> Dict[str, List[str]]:
    results = {}
    for header in headers:
        results[header] = []

    for row in data:
        for i, value in enumerate(row):
            results[headers[i]].append(value)
    return results


def save_to_parquet(df: pd.DataFrame, filename: str) -> None:
    check_and_create_folder(filename)
    df.to_parquet(f"{filename}.parquet")


def check_and_create_folder(file_path: str) -> None:
    if '/' in file_path:
        folder_path = '/'.join(file_path.split('/')[:-1])
    else:
        folder_path = file_path
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def extract_non_numeric(value: str) -> str:
    # Using regular expression to find non-numeric characters at the start or end of the string
    prefix = re.search(r'^\D+', value)
    suffix = re.search(r'\D+$', value)
    # Return the non-numeric characters found, or 'unit' if none are found
    return prefix.group() if prefix else (suffix.group() if suffix else 'unit')


def transform_numeric_types(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    value_columns = []
    for col in columns:
        if 'count' in col.lower() or 'amount' in col.lower():
            value_columns.append(col)

    for value_column in value_columns:
        df[f'{value_column}_type'] = df[value_column].apply(
            extract_non_numeric)
        df[value_column] = df[value_column].apply(extract_number)
        df[value_column] = pd.to_numeric(
            df[value_column], errors='coerce')
    return df
