# -*- encoding: utf-8 -*-
"""
Created on Tue May  7 22:29:48 2019

@author: пк
"""

# в консоли IPython      ->      !pip install influxdb
#  from influxdb import InfluxDBClient
import pandas as pd

#  import numpy as np

data = "Дата замера"
hole = "Скважина"
x1 = "Способ эксплуатации"
x2 = "Режим"
y1 = "Рпр(ТМ)"
y2 = "Рзаб(Рпр)"
y3 = "Рзаб(Нд)"
y4 = "Рзаб(иссл)"

file_name = "data.xlsx"


def read_all_sheets(file_name_excel):
    df = pd.DataFrame()
    xls = pd.ExcelFile(file_name_excel)
    for list_excel in xls.sheet_names:
        df = df.append(
            pd.read_excel(xls, list_excel, parse_dates=[data], index_col=data)
        )
    return df


df = read_all_sheets(file_name)
df.sort_index(inplace=True)

df.to_csv("data.csv")

print(df)
