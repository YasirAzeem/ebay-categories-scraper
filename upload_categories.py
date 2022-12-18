from slugify import slugify
import mysql.connector
import pandas
from tqdm import tqdm

df = pandas.read_csv('results.csv')
all_cats_text = df['category_tree'].tolist()
name = "ebay"
conn = mysql.connector.connect(host='localhost',
                                   database='findused_local',
                                   user='root',
                                   password='test12345')
cursor = conn.cursor(buffered=True)
conn.autocommit = False
for cat in tqdm(all_cats_text):
    all_cats_list = [x.strip() for x in cat.split('>')[1:] if x]

    indx_dict = {}
    for kn, cat in enumerate(all_cats_list):
        indx_dict[cat] = kn
    cats = ['"'+x.strip()+'"' for x in all_cats_list if x]
    sql_update_query = f'''SELECT * from findused_local.categories where category_name IN ({",".join(cats)}) AND type = "{name}";'''
    cursor.execute(sql_update_query)
    x = cursor.fetchall()

    cats_dict = {}
    for c in x:
        c = list(c)
        cats_dict[c[1]] = c[0]
    catid_list = [str(x) for x in list(cats_dict.values())]
    if len(cat.split('>'))==len(list(cats_dict.keys())):
        catid_list = ",".join(catid_list)
    else:
        cats_to_add = [c for c in all_cats_list if c not in list(cats_dict.keys())]
        for i,clr in enumerate(cats_to_add):
            ct_slug = slugify(clr)
            parent_id = None
            for k,ac in enumerate(all_cats_list):
                if clr==ac:
                    if k==0:
                        break
                    parent_id = cats_dict.get(all_cats_list[k-1])
                    if not parent_id:
                        parent_id = catid_list[-1]
                    break
            if not parent_id:
                sql_update_query = f'''INSERT IGNORE INTO findused_local.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}","{ct_slug}","{name}", {indx_dict[clr]}, null, 0, 1);'''
            else:
                sql_update_query = f'''INSERT IGNORE INTO findused_local.categories (category_name, slug, type, depth, parent_id, status, user_id) VALUES ("{clr}", "{ct_slug}","{name}", {indx_dict[clr]},{parent_id}, 0, 1);'''

            cursor.execute(sql_update_query)
            catid_list.append(cursor.lastrowid)
            cats_dict[clr] = cursor.lastrowid
        catid_list = ",".join([str(x) for x in catid_list])
    conn.commit()