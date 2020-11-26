"""
    do_everything.py
    author: Some Genius
    date: 2020-11-27

    Скрипт загружает xlsx файл, проводит его чистку,
    отбирает интересующие нас строки, добавляет их в
    InfluxDB.

"""

# Подключим библиотеки
import re  # Регулярные выражения (для чистки строк)
import pandas as pd  # Работа с датафреймом
import numpy as np  # pandas использует типы отсюда
from influxdb import DataFrameClient  # Клиент для работы датафрейма с Influx


########################
#  Загрузка xls файла  #
########################

# Имя файла для импорта
filename = "data.xlsx"
date_col = "Дата замера"


# Конкатенация листов xls файла
def read_all_sheets(file_name_excel):
    df = pd.DataFrame()
    xls = pd.ExcelFile(file_name_excel)
    for list_excel in xls.sheet_names:
        df = df.append(pd.read_excel(xls, list_excel, parse_dates=[date_col]))
    return df


df = read_all_sheets(filename)
df.sort_index(inplace=True)

# Сохраним результат в csv
df.to_csv("data.csv")


######################
#  Обработка данных  #
######################

# Откроем датафрейм
df = pd.read_csv("data.csv")

# Переименуем столбцы
df = df.rename(
    columns={
        "Дата замера": "date",
        "Скважина": "hole",
        "Способ эксплуатации": "x1",
        "Режим": "x2",
        "Рпр(ТМ)": "y1",
        "Рзаб(Рпр)": "y2",
        "Рзаб(Нд)": "y3",
        "Рзаб(иссл)": "y4",
    }
)

# Оставим только нужные столбцы
col_list = ["date", "hole", "x1", "x2", "y1", "y2", "y3", "y4"]
df = df[col_list]

# Установим индекс на дату
df = df.set_index("date")
df.index = pd.to_datetime(df.index)

# Удалим лишние пробелы в начале и конце значений
df_strings = df.select_dtypes(["object"])
df[df_strings.columns] = df_strings.apply(lambda x: x.str.strip())


# Убрать из названия скважины все буквы, дефис и часть после дефиса
def clean_hole(hole):
    return int(re.sub("(-[\d\D]*)|\D", "", hole))


# Добавим столбец с модифицированными названиями скважин
df["filtered_hole"] = df["hole"].apply(clean_hole)
mask = (df["filtered_hole"] > 150) & (df["filtered_hole"] <= 250)

# Теперь в датасете только скважины от 151 до 250
df = df[mask]

# Удалить полностью пустые столбцы
df = df.dropna(axis=1, how="all")

# Удалим пустые строки
df = df.drop(columns=["hole"]).dropna(axis=1, how="all")

# Заменим строковые значения на числа
df["x1"] = df["x1"].replace(
    {
        "Газлифт": 1,
        "Фонтанный": 2,
        "Электропогружным насосом": 3,
        "Прочие способы эксплуатации": 4,
        np.nan: 5,
    }
)

df["x2"] = df["x2"].replace({"АПВ": 1, "ПДФ": 2, "ПКВ": 3, np.nan: 4})

# Уберём дубликаты
df = df.drop_duplicates()

# Сохраним результат
df.to_csv("final.csv")


##########################################
#  Подключение к БД и добавление данных  #
##########################################

# Данные для подключения
db_host = "localhost"
db_name = "production"
db_port = 8086
db_user = "root"
db_password = "root"


def rewrite_data(db_client, db_name, dataframe, tag_column_name):
    """Drop existing table, add data with tag"""
    db_client.drop_database(db_name)
    db_client.create_database(db_name)

    for name, group in df.groupby(tag_column_name):
        db_client.write_points(group, db_name, {tag_column_name: name})


# Создать клиента к БД
db_client = DataFrameClient(db_host, db_port, db_user, db_password, db_name)

# Перезаписать наши данные в Influx
rewrite_data(db_client=db_client, db_name=db_name, dataframe=df, tag_column_name="filtered_hole")
