import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from googletrans import Translator

translator = Translator()

for num_of_rooms in range(1, 6):
    url = f"https://www.olx.uz/d/nedvizhimost/kvartiry/prodazha/tashkent/?currency=UYE&search%5Border%5D=created_at:desc&search%5Bfilter_float_number_of_rooms:from%5D={num_of_rooms}&search%5Bfilter_float_number_of_rooms:to%5D={num_of_rooms}&search%5Bfilter_enum_type_of_market%5D%5B0%5D=secondary"

    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    paginator = soup.select_one('.css-4mw0p4')
    last_page = int(paginator.select('a')[-2].text)

    url = "https://www.olx.uz/d/nedvizhimost/kvartiry/prodazha/tashkent/?currency=UYE&page={pg_number}&search%5Bfilter_enum_type_of_market%5D%5B0%5D=secondary&search%5Bfilter_float_number_of_rooms%3Afrom%5D={num_of_rooms}&search%5Bfilter_float_number_of_rooms%3Ato%5D={num_of_rooms}&search%5Border%5D=created_at%3Adesc"
    urls = []
    for i in range(1, last_page + 1):
        urls.append(url.format(pg_number=i, num_of_rooms=num_of_rooms))

    dfs = []
    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        data = []
        for row in soup.select('.css-oukcj3 .css-1sw7q4x'):
            columns = row.select('.css-16v5mdi, .css-10b0gli, .css-veheph, .css-643j0o')
            data.append([col.get_text(strip=True) for col in columns])

        dfs.append(pd.DataFrame(data, columns=['Column1', 'Column2', 'Column3', 'Column4']))

    df = pd.concat(dfs, ignore_index=True)

    df2 = df[df['Column3'].str.contains("Сегодня") == False]
    df2.rename(columns={'Column1': 'Ad_text'}, inplace=True)
    df2['Price'] = df2['Column2'].str.extract(r'(\d+\s?\d*)').replace(' ', '', regex=True).astype(int)
    # df2.drop('Column2', axis=1, inplace=True)
    del df2['Column2']
    df2[['City_district_rus', 'Date_rus']] = df2['Column3'].str.rsplit('-', n=1, expand=True)
    # df2.drop('Column3', axis=1, inplace=True)
    del df2['Column3']
    # df2 = df2[df2['Date'] == date_string]

    df2['Area'] = df2['Column4'].str.extract(r"(\d+\.\d+|\d+) м²", expand=False).astype(float)
    # df2.drop('Column4', axis=1, inplace=True)
    del df2['Column4']

    

    df2.loc[:, 'rooms'] = num_of_rooms
    df2.loc[:,'type'] = 'secondary'
    # df2['Title'] = df2.loc[:, 'Ad_text'].apply(lambda x: translator.translate(x, dest='en').text)
    df2['City_District'] = df2.loc[:, 'City_district_rus'].apply(lambda x: translator.translate(x, dest='en').text)
    df2['Date'] = df2.loc[:, 'Date_rus'].apply(lambda x: translator.translate(x, dest='en').text)

    del df2['City_district_rus']
    del df2['Ad_text']
    del df2['Date_rus']
    df3 = df2.drop_duplicates(subset=['Area','City_District', 'Price'])

    # # create a connection to the database
    conn = sqlite3.connect('my_db.db')

    # # write the data frame to the database
    df3.to_sql(name=f'secondary_building', con=conn, if_exists='append', index=False)

    # # close the database connection

    conn.close()
    # Specify the CSV file to write to
    csv_file = 'C:\\Data Scients\\Learn Data Science\\Kaggle projects\\1 Kaggle projects\\secondary building.csv'

    # Write the DataFrame to the CSV file in append mode
    df3.to_csv(csv_file, mode="a", index=False)

    # print(len(df3.index), df3)








