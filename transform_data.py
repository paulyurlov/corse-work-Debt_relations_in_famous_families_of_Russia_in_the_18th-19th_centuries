import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def transform_columns(sup):
    unique = set(sup)
    zeros = [0 for _ in range(len(unique))]
    dct = dict(zip(unique, zeros))
    for i, el in enumerate(sup):
        if el in unique:
            if dct[el] == 0:
                dct[el] += 1
            else:
                sup[i] = str(el) + '.' + str(dct[el])
    return sup


def rang_(input_string):
    sup = str(input_string).split(', ')
    return str(', '.join([str(x) + ' ранг' for x in sup]))


def trim_df(df):
    df.columns = list(map(lambda x: x.strip(), df.columns.tolist()))
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    df['Сумма долга'] = df['Сумма долга'].apply(lambda x: x.replace(',', '.'))
    df['Сумма долга'] = df['Сумма долга'].replace('неизвестно', 0)
    df['Сумма долга'] = df['Сумма долга'].astype(np.float64)
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.title())
    df['Ранг кредитора'] = df['Ранг кредитора'].astype('str')
    df['Ранг заемщика'] = df['Ранг заемщика'].astype('str')
    df['Ранг кредитора'] = df['Ранг кредитора'].apply(rang_)
    df['Ранг заемщика'] = df['Ранг заемщика'].apply(rang_)
    df['Дата'] = df['Дата'].replace('-', np.nan)
    df['Дата.1'] = df['Дата.1'].replace('-', np.nan)
    df['Дата'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y')
    df['Дата.1'] = pd.to_datetime(df['Дата.1'], format='%d.%m.%Y')
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.astype('str')


# Отличие trim_df_spec от trim_df в замене np.nan на Unknown и преобразование
# даты в строку (для правильной работы gspread)


def trim_df_spec(df):
    trim_df(df)
    df['Дата'] = df['Дата'].apply(lambda x: x.date())
    df['Дата.1'] = df['Дата.1'].apply(lambda x: x.date())
    df.fillna('Unknown', inplace=True)
    df['Дата'] = df['Дата'].astype('str')
    df['Дата.1'] = df['Дата.1'].astype('str')
    df.replace('nan', 'Unknown', inplace=True)
    df.replace('nan ранг', 'Unknown', inplace=True)


def transform_main(gc_, url_):   # Обрабатывает основную таблицу
    dataframe = download_main(gc_, url_)
    trim_df_spec(dataframe)
    worksheet = gc.open_by_url(url).worksheet('Main_trimmed')
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    worksheet.format("A1:AL1", {
        "backgroundColor": {
            "red": 0.648,
            "green": 0.792,
            "blue": 0.94
        }})
    worksheet.format("A1:AL10000", {
        "wrapStrategy": 'WRAP', "horizontalAlignment": "LEFT"})


def download_main(gc_, url_):
    sheet_main = gc_.open_by_url(url_).worksheet('Основная таблица')
    df = pd.DataFrame(sheet_main.get_all_values())
    cols = list(map(lambda x: x.strip(), df.loc[0].values.tolist()))
    columns_to_use = transform_columns(cols)
    df.drop(0, inplace=True)
    df.replace('', np.nan, inplace=True)
    new_df = pd.DataFrame(dict(zip(columns_to_use, df.values.T.tolist())))
    dataframe = new_df.copy()
    return dataframe


def get_sparse_matrix(df):
    new_df = pd.DataFrame()
    trim_df(df)
    df = df[['Заемщик', 'Кредитор', 'Сумма долга']]
    for i, row in df.iterrows():
        new_df.loc[row[0] + '_' + str(i), row[1] + '_' + str(i)] = row[2]
    support = [x.split("_")[0] for x in new_df.columns.values.tolist()]
    df_transformed = pd.DataFrame([['Заемщик\Кредитор'] + support])
    i = 1
    for nam, row in new_df.iterrows():
        df_transformed.loc[i] = [nam.split('_')[0]] + row.values.tolist()
        i += 1
    df_transformed.fillna(int(0), inplace=True)
    worksheet = gc.open_by_url(url).worksheet('Перекрестная таблица')
    worksheet.update(df_transformed.values.tolist())


def extract(pand_lst):
    result = list()
    names = str(pand_lst[1]).split(', ')
    chin = str(pand_lst[2]).split(', ')
    rang = str(pand_lst[3]).split(', ')
    titul = str(pand_lst[4]).split(', ')
    names = list(map(lambda x: x.strip(), names))
    mx = max(len(names), len(chin), len(rang), len(titul))
    for i in range(mx - len(names)):
        names.append('None')
    for i in range(mx - len(chin)):
        chin.append('None')
    for i in range(mx - len(rang)):
        rang.append('None')
    for i in range(mx - len(titul)):
        titul.append('None')

    for zipped in zip(names, chin, rang, titul):
        result.append(zipped)
    return result


def if_group(pas):
    names = str(pas[1]).split(', ')
    if len(names) > 1:
        return True
    return False


def get_ids(df):
    df.fillna('Unknown', inplace=True)
    debtors = df[['Заемщик', 'Чин заемщика', 'Ранг заемщика', 'Титул заемщика', 'Сословие']]
    creditors = df[['Кредитор', 'Чин кредитора', 'Ранг кредитора', 'Титул кредитора', 'Сословие.1']]
    uni = set()
    for row in debtors.itertuples():
        for el in extract(row):
            uni.add(el)

    for row in creditors.itertuples():
        for el in extract(row):
            uni.add(el)
    d = {'Имя': [], 'Чин': [], 'Ранг': [], 'Титул': []}
    df_id = pd.DataFrame(data=d)
    for i, el in enumerate(uni):
        df_id.loc[i] = el
    df_id['id'] = ['id_p_' + str(x) for x in range(df_id.shape[0])]
    uni_groups = set()
    for row in debtors.itertuples():
        if if_group(row):
            uni_groups.add((row[1], row[2], row[3], row[4]))

    for row in creditors.itertuples():
        if if_group(row):
            uni_groups.add((row[1], row[2], row[3], row[4]))

    for i, el in enumerate(uni_groups):
        shpe = df_id.shape[0]
        df_id.loc[i + shpe, ['Имя', 'Чин', 'Ранг', 'Титул']] = el
        df_id.loc[i + shpe, 'id'] = 'id_gr_' + str(i)
    df_id['Ранг'].replace('None', 'Unknown', inplace=True)
    df_id['Титул'].replace('None', 'Unknown', inplace=True)
    df_id['Титул'] = df_id['Титул'].apply(lambda x: x.title())
    df_id['Ранг'] = df_id['Ранг'].apply(lambda x: rang_(x) if x != 'Unknown' else 'Unknown')
    df_id.sort_values(by=['Имя'], inplace=True)
    worksheet = gc.open_by_url(url).worksheet('id')
    worksheet.update([df_id.columns.values.tolist()] + df_id.values.tolist())
    worksheet.format("A1:E1", {
        "backgroundColor": {
            "red": 0.248,
            "green": 0.792,
            "blue": 0.94
        }})
    worksheet.format("A1:E10000", {
        "wrapStrategy": 'WRAP', "horizontalAlignment": "LEFT"})



CREDENTIALS_FILE = 'service_account.json'

credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE,
                                                               ['https://www.googleapis.com/auth/spreadsheets'])
gc = gspread.authorize(credentials)
url = 'https://docs.google.com/spreadsheets/d/1MiQ7UPF9T9YXzaadFONsuoXQJUTPs1k1XREEqXfmNzw/edit#gid=1488630400'

transform_main(gc, url)
get_sparse_matrix(download_main(gc, url))
get_ids(download_main(gc, url))
