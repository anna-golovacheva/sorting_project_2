import os
import pandas as pd


def select_sorted(sort_columns: list, order: str, filename: str, limit=None) -> None:
    """
    Функция сортирует данные по выбранной колонке. Отсортированные данные в
    выбранном порядке сохраняются в файл по выбранному адресу.
    :param sort_columns: list
    :param order: str
    :param filename: str
    :param limit: int | None
    :return: None
    """
    path_for_starter_file = 'data/all_stocks_5yr.csv'
    path_for_hash = f'data/hash/select_sorted_{"_".join(sort_columns)}_{order}_{limit}.csv'
    if os.path.exists(path_for_hash):
        df = pd.read_csv(path_for_hash)
    else:
        ascending_value = False if order == 'desc' else True
        df = pd.read_csv(path_for_starter_file)
        df.sort_values(by=sort_columns, inplace=True, na_position='first', ascending=ascending_value)
        if limit:
            df = df.iloc[:limit]
        df.to_csv(path_for_hash, index=False)
    df.to_csv(filename, index=False)


def get_by_date(date: str, name: str, filename: str) -> None:
    """
    Функция записывает результат запроса с выбранными датой и названием тикера в выбранный файл.
    :param date: str
    :param name: str
    :param filename: str
    :return: None
    """
    path_for_hash = f'data/hash/get_by_date_{date}_{name}.csv'

    if os.path.exists(path_for_hash):
        df = pd.read_csv(path_for_hash)
    else:
        df = pd.read_csv('data/sorted_dump.csv')

        if date != 'all' and name != 'all':
            df.query('date == @date & Name == @name', inplace=True)
        elif date == 'all' and name != 'all':
            df.query('Name == @name', inplace=True)
        elif date != 'all' and name == 'all':
            df.query('date == @date', inplace=True)

        df.to_csv(path_for_hash, index=False)

    df.to_csv(filename, index=False)


def run_sorting() -> None:
    """
    Функция позволяет выбрать параметры вызова функции select_sorted.
    :return: None
    """
    sorting_dict = {
        '0': 'date',
        '1': 'open',
        '2': 'high',
        '3': 'low',
        '4': 'close',
        '5': 'volume',
        '6': 'Name'
    }
    sort_column = input('Choose sorting:/nby date (0)\nby open price (1)\nby high price [2]\nby low price (3)\nby close price (4)\nby volume (5)\nby name (6) > ')
    sort_by = sorting_dict['2'] if sort_column == '' or '2' else sorting_dict[sort_column]

    order = input('Choose order of data: descending [1] / ascending (2) > ')
    order_by = 'desc' if order == '' or '1' else 'asc'

    limit = input('Choose limit of data [10]: > ')
    limited = None if limit == 'None' else (10 if limit == '' else int(limit))

    file = input('Choose file name for data dump [dump.csv]: > ')
    if file == '':
        file = 'dump.csv'
    file_path = 'data/' + file

    select_sorted(sort_columns=[sort_by], order=order_by, limit=limited, filename=file_path)

    print(f'data saved to {file_path}')


def run_getting_by_date() -> None:
    """
    Функция позволяет выбрать параметры вызова функции get_by_date.
    :return: None
    """
    date = input('Choose date as yyyy-mm-dd [all]: > ')
    if date == '':
        date = 'all'
    ticker = input('Choose ticker [all]: > ')
    if ticker == '':
        ticker = 'all'
    file = input('Choose file name for data dump [dump_date.csv]: > ')
    if file == '':
        file = 'dump_by_date.csv'
    file_path = 'data/' + file

    get_by_date(date=date, name=ticker, filename=file_path)

    print(f'data saved to {file_path}')


if __name__ == '__main__':
    run_sorting()
    run_getting_by_date()
