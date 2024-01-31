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

df_restaurants[["restaurant_id","rating","califications_comida", "califications_servicio", "califications_calidad_precio","califications_ambiente","traveler_rating_excelente","traveler_rating_muy_bueno","vegano","vegetariano","sin_gluten"]] = df_restaurants[["restaurant_id","rating","califications_comida", "califications_servicio", "califications_calidad_precio","califications_ambiente","traveler_rating_excelente","traveler_rating_muy_bueno","vegano","vegetariano","sin_gluten"]].fillna(" ")

# TODO: ver field_boost en wh.fields
schema = wh.fields.Schema(
    restaurant_id=wh.fields.ID(stored=True),
    rating=wh.fields.ID(),
    califications_comida=wh.fields.ID(),
    califications_servicio=wh.fields.ID(),
    califications_calidad_precio=wh.fields.ID(),
    califications_ambiente=wh.fields.ID(),
    traveler_rating_excelente=wh.fields.ID(),
    traveler_rating_muy_bueno=wh.fields.ID(),
    vegano=wh.fields.ID(),
    vegetariano=wh.fields.ID(),
    sin_gluten=wh.fields.ID()
)

ix = wh.index.create_in("indexdir", schema)

writer = ix.writer()
for index, row in df_restaurants.iterrows():
    writer.add_document(id_libro=row["id_libro"],
                        autor=row["autor"],
                        editorial=row["editorial"],
                        genero=row["genero"]
    )
writer.commit()


terminos = [wh.query.Term("editorial", "DEBOLSILLO"), wh.query.Term("editorial", "PLANETA")]
query = wh.query.Or(terminos)

with ix.searcher() as searcher:
    results = searcher.search(query, terms=True)
    for r in results:
        print(r)
