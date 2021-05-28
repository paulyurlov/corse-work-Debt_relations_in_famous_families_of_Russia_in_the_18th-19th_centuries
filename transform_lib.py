import numpy as np
import pandas as pd
import warnings
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def transform_names(x):  # Приводит имена к одному формату
    tmp = x
    #print(x)
    tmp.lower()
    tmp = tmp.split()
    tmp = [sup.title() for sup in tmp]
    new_str = ''
    for el in tmp:
        new_str += el
        new_str += ' '
    new_str = new_str.strip()
    new_str = new_str.strip()
    return new_str


def rang_(input_string):  # Делает из номера ранга строку 'x ранг'
    sup = str(input_string).split(', ')
    if sup[0] == '0.0' or sup[0] == '0':
        return '-'
    return str(', '.join([str(x.replace('.0', '')) + ' ранг' for x in sup]))


def clean_rangs(df):  # Приводит ранги к одному формату
    df['Ранг кредитора'].replace(' ранг', '', inplace=True)
    df['Ранг заемщика'].replace(' ранг', '', inplace=True)
    df['Ранг заемщика'].fillna(0, inplace=True)
    df['Ранг кредитора'].fillna(0, inplace=True)
    df['Ранг кредитора'] = df['Ранг кредитора'].astype('str')
    df['Ранг заемщика'] = df['Ранг заемщика'].astype('str')
    df['Ранг кредитора'] = df['Ранг кредитора'].apply(rang_)
    df['Ранг заемщика'] = df['Ранг заемщика'].apply(rang_)


def transform_title(df):  # Приводит все строковые колонки к одному формату, кроме имен
    to_title = ['Пол заемщика', 'Чин заемщика', 'Титул заемщика', 'Чин кредитора', 'Титул кредитора',
                'Семейное положение', 'Семейное положение.1', 'Сословие заемщика', 'Сословие кредитора']
    for el in to_title:
        df[el] = df[el].apply(lambda x: str(x).strip().lower().title())


def transform_prices(x):  # Приводит цены в один формат
    tmp = str(x)
    if ',' in tmp[-3:] or '.' in tmp[-3:]:
        tmp = tmp[:-3]
    tmp = tmp.replace(',', '')
    tmp = tmp.replace(' ', '')
    tmp = ''.join(tmp.split())
    tmp = ''.join([i for i in tmp if i.isdigit()])
    return tmp


def clean_data(df):  # Чистит данные
    df['Сумма долга'].replace('16 душ', '0', inplace=True)
    df['Сумма долга'].replace('', '0', inplace=True)
    df['Сумма долга'].fillna(0, inplace=True)
    df['Сумма долга'] = df['Сумма долга'].apply(transform_prices)
    df['Сумма долга'].replace('', '0', inplace=True)
    df['Сумма долга'] = df['Сумма долга'].astype(np.float64)
    df['Заемщик'] = df['Заемщик'].apply(transform_names)
    df['Кредитор'] = df['Кредитор'].apply(transform_names)
    transform_title(df)
    clean_rangs(df)
    df['Дата сделки'] = df['Дата сделки'].replace('-', np.nan)
    df['Дата закрытия'] = df['Дата закрытия'].replace('-', np.nan)
    df['Дата сделки'] = pd.to_datetime(df['Дата сделки'], format='%d.%m.%Y')
    df['Дата закрытия'] = pd.to_datetime(df['Дата закрытия'], format='%d.%m.%Y')
    df['Пол заемщика'] = df['Пол заемщика'].replace('M', 'М')
    df['Пол кредитора'] = df['Пол кредитора'].replace('M', 'М')
    df['Пол заемщика'] = df['Пол заемщика'].apply(lambda x: x.strip().upper())
    df['Пол кредитора'] = df['Пол кредитора'].apply(lambda x: x.strip().upper())


def fill_id(df):  # заполняет df id
    tmp = get_ids(df)
    tmp.sort_values('Имя')
    tmp.to_excel('id_table.xlsx')

    tmp.replace('None', np.nan, inplace=True)
    tmp.replace('-', np.nan, inplace=True)
    tmp.replace('', np.nan, inplace=True)
    tmp.replace('Nan', np.nan, inplace=True)
    tmp.replace('0', np.nan, inplace=True)
    tmp.replace('Unknown', np.nan, inplace=True)
    tmp.fillna('-', inplace=True)

    df.replace('None', np.nan, inplace=True)
    df.replace('-', np.nan, inplace=True)
    df.replace('', np.nan, inplace=True)
    df.replace('Nan', np.nan, inplace=True)
    df.replace('0', np.nan, inplace=True)
    df.replace('Unknown', np.nan, inplace=True)
    df.fillna('-', inplace=True)

    dct = dict(zip(tuple(zip(tmp['Имя'].values.tolist(), tmp['Чин'].values.tolist(), tmp['Ранг'].values.tolist(),
                             tmp['Титул'].values.tolist())), tmp['id'].values.tolist()))
    df.to_excel('id_added.xlsx')

    for row in df.iterrows():
        row = row[1]
        debtor = (row['Заемщик'], row['Чин заемщика'], row['Ранг заемщика'], row['Титул заемщика'])
        creditor = (row['Кредитор'], row['Чин кредитора'], row['Ранг кредитора'], row['Титул кредитора'])
        if debtor in dct:
            df.loc[(df['Заемщик'] == row['Заемщик']) & (df['Чин заемщика'] == row['Чин заемщика']) &
                   (df['Ранг заемщика'] == row['Ранг заемщика']) & (
                               df['Титул заемщика'] == row['Титул заемщика']), 'ID заемщика'] = dct[debtor]
        else:
            print(debtor)
            raise ValueError('Something went wrong debtor')
        if creditor in dct:
            df.loc[(df['Кредитор'] == row['Кредитор']) & (df['Чин кредитора'] == row['Чин кредитора']) &
                   (df['Ранг кредитора'] == row['Ранг кредитора']) & (
                               df['Титул кредитора'] == row['Титул кредитора']), 'ID кредитора'] = dct[creditor]
        else:
            raise ValueError('Something went wrong creditor')

    print('Таблица с id сохранена в id_table.xlsx, Таблица с измененными id сохранена в id_added.xlsx')
    print('Необходимо копировать только столбцы ID!')
    warnings.warn("НЕ КОПИРОВАТЬ ВСЮ ТАБЛИЦУ!")


def get_sparse_matrix_names(df):  #  Возвращает sparse матрицу имен
    new_df = pd.DataFrame()
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
    return df_transformed
    # worksheet = gc_.open_by_url(url_).worksheet('Перекрестная таблица')
    # worksheet.update(df_transformed.values.tolist())


def get_sparse_matrix(df):  #  Возвращает sparse матрицу id
    new_df = pd.DataFrame()
    df = df[['ID заемщика', 'ID кредитора', 'Сумма долга']]
    for i, row in df.iterrows():
        new_df.loc[row[0] + '>' + str(i), row[1] + '>' + str(i)] = row[2]
    support = [x.split('>')[0] for x in new_df.columns.values.tolist()]
    df_transformed = pd.DataFrame([['Заемщик\Кредитор'] + support])
    i = 1
    for nam, row in new_df.iterrows():
        df_transformed.loc[i] = [nam.split('>')[0]] + row.values.tolist()
        i += 1
    df_transformed.fillna(int(0), inplace=True)
    return df_transformed


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


def get_ids(df):  # Генерирует DataFrame с id
    df.fillna('Unknown', inplace=True)
    debtors = df[['Заемщик', 'Чин заемщика', 'Ранг заемщика', 'Титул заемщика', 'Сословие заемщика']]
    creditors = df[['Кредитор', 'Чин кредитора', 'Ранг кредитора', 'Титул кредитора', 'Сословие кредитора']]
    uni = set()
    for row in debtors.itertuples():
        for el in extract(row):
            uni.add(el)

    for row in creditors.itertuples():
        for el in extract(row):
            uni.add(el)
    d = {'Имя': [], 'Чин': [], 'Ранг': [], 'Титул': []}
    df_id = pd.DataFrame(data=d)
    uni = sorted(uni)
    for i, el in enumerate(uni):
        df_id.loc[i] = el
    df_id['id'] = ['id_personal_' + str(x) for x in range(df_id.shape[0])]
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
        df_id.loc[i + shpe, 'id'] = 'id_group_' + str(i)
    df_id['Ранг'].replace('None', 'Unknown', inplace=True)
    df_id['Титул'].replace('None', 'Unknown', inplace=True)
    df_id['Титул'] = df_id['Титул'].apply(lambda x: x.title())
    df_id['Ранг'] = df_id['Ранг'].apply(lambda x: x if x != 'Unknown' else '-')
    df_id.sort_values(by=['Имя'], inplace=True)
    return df_id


def export_table(df, gc_, url_, sheet):  # Загружает в google sheets по url_ обычную таблицу
    tab = gc_.open_by_url(url_)
    if sheet in tab.worksheets():
        tab.del_worksheet(sheet)
    _ = tab.add_worksheet(title=sheet, rows=df.shape[0], cols=df.shape[0] + 10)
    worksheet = tab.worksheet(sheet)
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())


def export_sparce(df, gc_, url_, sheet):  # Загружает в google sheets по url_ таблицу смежности
    tab = gc_.open_by_url(url_)
    tab.del_worksheet(sheet)
    _ = tab.add_worksheet(title=sheet, rows=df.shape[0], cols=df.shape[0] + 10)
    worksheet = tab.worksheet(sheet)
    worksheet.update(df.values.tolist())


