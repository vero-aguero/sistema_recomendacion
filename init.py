import sqlite3
import os

import pandas as pd

import whoosh as wh
from whoosh import fields
from whoosh import index
from whoosh import qparser

THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))

con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
df_restaurants = pd.read_sql_query("SELECT * FROM restaurants", con)
con.close()

df_restaurants[["restaurant_id","type","special_diets"]] = df_restaurants[["restaurant_id","type","special_diets"]].fillna(" ")

# TODO: ver field_boost en wh.fields
# TODO: ver field_boost en wh.fields
schema = wh.fields.Schema(
    restaurant_id=wh.fields.ID(stored=True),
    type=wh.fields.ID()
)

ix = wh.index.create_in("indexdir", schema)

writer = ix.writer()
for index, row in df_restaurants.iterrows():
    writer.add_document(restaurant_id=int(row["restaurant_id"]),
                        type=row["type"]
    )
writer.commit()


terminos = [wh.query.Term("type", "Café"), wh.query.Term("type", "Panadería")]
query = wh.query.Or(terminos)

with ix.searcher() as searcher:
    results = searcher.search(query, terms=True)
    for r in results:
        print(r)
