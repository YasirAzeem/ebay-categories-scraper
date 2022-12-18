
import mysql.connector
import pandas
from tqdm import tqdm

df = pandas.read_csv('main-db-keywords-list.csv',encoding='ISO-8859-1')
df = df[['keyword', 'slug']]
values1 = list([list(x) for x in df.to_records(index=False)])



conn = mysql.connector.connect(host='localhost',
                                   database='findused_local',
                                   user='root',
                                   password='test12345')
cursor = conn.cursor(buffered=True)
conn.autocommit = False
for cat in tqdm(values1):

    command = "INSERT IGNORE INTO findused_local.all_keywords (keyword, slug) values (%s, %s);"
    cursor.executemany(command, values1)
    conn.commit()
