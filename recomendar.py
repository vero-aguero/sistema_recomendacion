import sqlite3
import pandas as pd
import sys
import os

import lightfm as lfm
from lightfm import data
from lightfm import cross_validation
from lightfm import evaluation
import surprise as sp

import whoosh as wh
from whoosh import fields
from whoosh import index
from whoosh import qparser

import utils


THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))
#THIS_FOLDER = '/home/veroaguero/sistema_recomendacion/'

def recomendar_top_9(user_id, interacciones="interactions"):
    query = f"""
        SELECT restaurant_id, AVG(rating) as rating, count(*) AS cant
          FROM {interacciones}
         WHERE restaurant_id NOT IN (SELECT restaurant_id FROM {interacciones} WHERE user_id = ?)
           AND rating > 0
         GROUP BY 1
         ORDER BY 3 DESC, 2 DESC
         LIMIT 9
    """
    restaurant_id = [r["restaurant_id"] for r in utils.sql_select(query, (user_id,))]
    return restaurant_id


def recomendar_perfil(user_id, interacciones="interactions"):
    # TODO: usar otras columnas además de genlit
    # TODO: usar datos del usuario para el perfil
    # TODO: usar cantidad de interacciones para desempatar los scores de perfil iguales
    # TODO: usar los items ignorados

    user_id=str(user_id)
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    df_interacciones = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_restaurants = pd.read_sql_query("SELECT * FROM restaurants", con)
    #df_users = pd.read_sql_query("SELECT * FROM users", con)
    con.close()

    print(user_id)
    df_restaurants['rating'].fillna(0, inplace=True)
    df_restaurants['califications_comida'].fillna(0, inplace=True)
    df_restaurants['califications_servicio'].fillna(0, inplace=True)
    df_restaurants['califications_calidad_precio'].fillna(0, inplace=True)
    df_restaurants['califications_ambiente'].fillna(0, inplace=True)

    df_restaurants = df_restaurants.rename(columns={"rating": "rating_rest"})
    perf_restaurants = df_restaurants[["restaurant_id","rating_rest","califications_comida","califications_servicio","califications_calidad_precio","califications_ambiente"]]

    perf_usuario = df_interacciones[(df_interacciones["user_id"] == user_id) & (df_interacciones["rating"] > 0)].merge(perf_restaurants, on="restaurant_id")

    for c in perf_usuario.columns:
        if c.startswith("califications_"):
            perf_usuario[c] = perf_usuario[c] * perf_usuario["rating"]


    perf_usuario = perf_usuario.drop(columns=["restaurant_id","rating","rating_date","title","opinion","processed_opinion","processed_title"]).groupby("user_id").mean()
    #print(perf_usuario.head(20))
    perf_usuario = perf_usuario / perf_usuario.sum(axis=1)[0] # normalizo
    for g in perf_usuario.columns:
        perf_restaurants[g] = perf_restaurants[g] * perf_usuario[g][0]

    #print(perf_restaurants.info())
    perf_restaurants.set_index('restaurant_id', inplace=True)
    restaurantes_vistos_o_puntuados = df_interacciones.loc[df_interacciones["user_id"] == user_id, "restaurant_id"].tolist()
    recomendaciones = [restaurant for restaurant in perf_restaurants.sum(axis=1).sort_values(ascending=False).index
                       if restaurant not in restaurantes_vistos_o_puntuados][:9]
    #print("recomendaciones: ",recomendaciones)
    return recomendaciones



def recomendar_perfil_v2(user_id, interacciones="interactions"):
    # TODO: usar otras columnas además de genlit
    # TODO: usar datos del usuario para el perfil
    # TODO: usar cantidad de interacciones para desempatar los scores de perfil iguales
    # TODO: usar los items ignorados

    user_id=str(user_id)
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    df_interacciones = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_restaurants = pd.read_sql_query("SELECT * FROM restaurants", con)
    #df_users = pd.read_sql_query("SELECT * FROM users", con)
    con.close()

    print(user_id)
    df_restaurants['rating'].fillna(0, inplace=True)
    df_restaurants['latitude'].fillna(0, inplace=True)
    df_restaurants['longitude'].fillna(0, inplace=True)
    df_restaurants['califications_comida'].fillna(0, inplace=True)
    df_restaurants['califications_servicio'].fillna(0, inplace=True)
    df_restaurants['califications_calidad_precio'].fillna(0, inplace=True)
    df_restaurants['califications_ambiente'].fillna(0, inplace=True)

    df_restaurants = df_restaurants.rename(columns={"rating": "rating_rest"})
    perf_restaurants = df_restaurants[["restaurant_id","rating_rest","latitude","longitude","califications_comida","califications_servicio","califications_calidad_precio","califications_ambiente"]]

    perf_usuario = df_interacciones[(df_interacciones["user_id"] == user_id) & (df_interacciones["rating"] > 0)].merge(perf_restaurants, on="restaurant_id")

    for c in perf_usuario.columns:
        if c.startswith("califications_"):
            perf_usuario[c] = perf_usuario[c] * perf_usuario["rating"]


    perf_usuario = perf_usuario.drop(columns=["restaurant_id","rating","rating_date","title","opinion","processed_opinion","processed_title"]).groupby("user_id").mean()
    #print(perf_usuario.head(20))
    perf_usuario = perf_usuario / perf_usuario.sum(axis=1)[0] # normalizo
    for g in perf_usuario.columns:
        perf_restaurants[g] = perf_restaurants[g] * perf_usuario[g][0]

    #print(perf_restaurants.info())
    perf_restaurants.set_index('restaurant_id', inplace=True)
    restaurantes_vistos_o_puntuados = df_interacciones.loc[df_interacciones["user_id"] == user_id, "restaurant_id"].tolist()
    recomendaciones = [restaurant for restaurant in perf_restaurants.sum(axis=1).sort_values(ascending=False).index
                       if restaurant not in restaurantes_vistos_o_puntuados][:9]
    #print("recomendaciones: ",recomendaciones)
    return recomendaciones


def recomendar_lightfm(user_id, interacciones="interactions"):
    # TODO: optimizar hiperparámetros
    # TODO: entrenar el modelo de forma parcial
    # TODO: user item_features y user_features
    # TODO: usar los items ignorados (usar pesos)

    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    df_interacciones = pd.read_sql_query(f"SELECT * FROM {interacciones} WHERE rating > 0 and restaurant_id is not null", con)
    df_restaurants = pd.read_sql_query("SELECT * FROM restaurants WHERE restaurant_id is not null", con)
    con.close()

    ds = lfm.data.Dataset()
    ds.fit(users=df_interacciones["user_id"].unique(), items=df_restaurants["restaurant_id"].unique())
    
    user_id_map, user_feature_map, item_id_map, item_feature_map = ds.mapping()
    #print("recomendar_lightfm df_interacciones: ", df_interacciones.info())
    #print("df_interacciones nulos: ", df_interacciones[["user_id", "restaurant_id", "rating"]].isna().sum())
    (interactions, weights) = ds.build_interactions(df_interacciones[["user_id", "restaurant_id", "rating"]].itertuples(index=False))

    model = lfm.LightFM(no_components=20, k=5, n=10, learning_schedule='adagrad', loss='logistic', learning_rate=0.05, rho=0.95, epsilon=1e-06, item_alpha=0.0, user_alpha=0.0, max_sampled=10, random_state=42)
    model = lfm.LightFM(k=5, n=10, learning_schedule='adagrad', loss='logistic', learning_rate=0.05, rho=0.95, epsilon=1e-06, item_alpha=0.0, user_alpha=0.0, max_sampled=10, random_state=42)

    model.fit(interactions, sample_weight=weights, epochs=10)

    restaurants_visitados = df_interacciones.loc[df_interacciones["user_id"] == user_id, "restaurant_id"].tolist()
    todos_los_restaurants = df_restaurants["restaurant_id"].tolist()
    restaurants_no_visitados = set(todos_los_restaurants).difference(restaurants_visitados)
    predicciones = model.predict(user_id_map[user_id], [item_id_map[l] for l in restaurants_no_visitados])

    recomendaciones = sorted([(p, l) for (p, l) in zip(predicciones, restaurants_no_visitados)], reverse=True)[:9]
    recomendaciones = [restaurant[1] for restaurant in recomendaciones]
    return recomendaciones


def generate_feature_list(df, columns):
    '''
    Generate the list of features of corresponding columns to list
    In order to fit the lightdm Dataset
    '''
    features = df[columns].apply(
        lambda x: ','.join(x.map(str)), axis = 1)
    features = features.str.split(',')
    features = features.apply(pd.Series).stack().reset_index(drop = True)
    return features


def prepare_item_features(df, columns, id_col_name):
    '''
    Prepare the corresponding feature formats for 
    the lightdm.dataset's build_item_features function
    '''
    features = df[columns].apply(
            lambda x: ','.join(x.map(str)), axis = 1)
    features = features.str.split(',')
    features = list(zip(df[id_col_name], features))
    return features



def recomendar_lightfm_with_features(user_id, interacciones="interactions"):

    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    df_interacciones = pd.read_sql_query(f"SELECT * FROM {interacciones} WHERE rating > 0 and restaurant_id is not null", con)
    df_restaurants = pd.read_sql_query("SELECT * FROM restaurants WHERE restaurant_id is not null", con)
    con.close()

    df_restaurants['rating'].fillna(0, inplace=True)
    df_restaurants['califications_comida'].fillna(0, inplace=True)
    df_restaurants['califications_servicio'].fillna(0, inplace=True)
    df_restaurants['califications_calidad_precio'].fillna(0, inplace=True)
    df_restaurants['califications_ambiente'].fillna(0, inplace=True)
    df_restaurants = df_restaurants.rename(columns={"rating": "rating_rest"})

    df_restaurants = df_restaurants[["restaurant_id", "rating_rest", "califications_comida", "califications_servicio", "califications_calidad_precio","califications_ambiente"]]

    print(df_restaurants.info())

    ds = lfm.data.Dataset()
    
    
    #user_features = ds.build_user_features((row["user_id"], [row["activity_level"], row["cuisine_preference"]]) for row in df_users.itertuples(index=False))

    for row in df_restaurants.itertuples(index=False):
        print(row)
    # Construir la matriz de características de restaurante
    columns = ["rating_rest", "califications_comida", "califications_servicio", "califications_calidad_precio","califications_ambiente"]
    fitting_item_features = generate_feature_list(df_restaurants, columns)
    lightdm_features = prepare_item_features(df_restaurants, columns, 'restaurant_id')

    ds.fit(users=df_interacciones["user_id"].unique(), items=df_restaurants["restaurant_id"].unique(), item_features = fitting_item_features)
    item_features = ds.build_item_features(lightdm_features, 
                                                normalize = True)

    user_id_map, user_feature_map, item_id_map, item_feature_map = ds.mapping()
    print("recomendar_lightfm df_interacciones: ", df_interacciones.info())
    print("df_interacciones nulos: ", df_interacciones[["user_id", "restaurant_id", "rating"]].isna().sum())
    (interactions, weights) = ds.build_interactions(df_interacciones[["user_id", "restaurant_id", "rating"]].itertuples(index=False))

    model = lfm.LightFM(k=5, n=10, learning_schedule='adagrad', loss='logistic', learning_rate=0.05, rho=0.95, epsilon=1e-06, item_alpha=0.0, user_alpha=0.0, max_sampled=10, random_state=42)
    model.fit(interactions, sample_weight=weights, item_features=item_features, epochs=10)

    restaurants_visitados = df_interacciones.loc[df_interacciones["user_id"] == user_id, "restaurant_id"].tolist()
    todos_los_restaurants = df_restaurants["restaurant_id"].tolist()
    restaurants_no_visitados = set(todos_los_restaurants).difference(restaurants_visitados)
    predicciones = model.predict(user_id_map[user_id], [item_id_map[l] for l in restaurants_no_visitados])

    recomendaciones = sorted([(p, l) for (p, l) in zip(predicciones, restaurants_no_visitados)], reverse=True)[:9]
    recomendaciones = [restaurant[1] for restaurant in recomendaciones]
    return recomendaciones




def recomendar_surprise(user_id, interacciones="interactions"):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    df_int = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_items = pd.read_sql_query("SELECT * FROM restaurants", con)
    con.close()
    
    reader = sp.reader.Reader(rating_scale=(1, 10))

    data = sp.dataset.Dataset.load_from_df(df_int.loc[df_int["rating"] > 0, ['user_id', 'restaurant_id', 'rating']], reader)
    trainset = data.build_full_trainset()
    model = sp.prediction_algorithms.matrix_factorization.SVD(n_factors=300, n_epochs=40, random_state=43)
    model.fit(trainset)

    restaurants_visitados_o_vistos = df_int.loc[df_int["user_id"] == user_id, "restaurant_id"].tolist()
    todos_los_restaurants = df_items["restaurant_id"].tolist()
    restaurants_no_visitados_ni_vistos = set(todos_los_restaurants).difference(restaurants_visitados_o_vistos)
    
    predicciones = [model.predict(user_id, l).est for l in restaurants_no_visitados_ni_vistos]
    recomendaciones = sorted([(p, l) for (p, l) in zip(predicciones, restaurants_no_visitados_ni_vistos)], reverse=True)[:9]

    recomendaciones = [restaurant[1] for restaurant in recomendaciones]
    return recomendaciones


def recomendar_whoosh(user_id, interacciones="interactions"):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    df_interacciones = pd.read_sql_query(f"SELECT * FROM {interacciones}", con)
    df_items = pd.read_sql_query("SELECT * FROM restaurants", con)
    con.close()

    user_id=str(user_id)
    print("UserID:"+user_id)
    # TODO: usar cant
    terminos = []    
    for campo in ["califications_comida", "califications_servicio", "califications_calidad_precio","califications_ambiente"]:
        query = f"""
            SELECT {campo} AS valor, count(1) AS cant
            FROM interactions AS i JOIN restaurants AS r ON i.restaurant_id = r.restaurant_id
            WHERE user_id = ?
            AND i.rating > 0
            GROUP BY {campo}
            HAVING cant > 1
            ORDER BY cant DESC
            LIMIT 3
        """       
        rows = utils.sql_select(query, (user_id,))

        for row in rows:
            terminos.append(wh.query.Term(campo, row["valor"]))
    
    query = wh.query.Or(terminos)

    restaurantes_vistos_o_puntuados = df_interacciones.loc[df_interacciones["user_id"] == user_id, "restaurant_id"].tolist()

    # TODO: usar el scoring
    # TODO: ampliar la busqueda con autores parecidos (matriz de similitudes de autores)
    ix = wh.index.open_dir("indexdir")
    with ix.searcher() as searcher:
        results = searcher.search(query, terms=True, scored=True, limit=1000)
        recomendaciones = [r["restaurant_id"] for r in results if r not in restaurantes_vistos_o_puntuados][:9]

    return recomendaciones

def recomendar(user_id, interacciones="interactions"):
    # TODO: combinar mejor los recomendadores
    # TODO: crear usuarios fans para llenar la matriz    

    
    cant_valorados = len(utils.valorados(user_id, interacciones))
    print(cant_valorados)

    if cant_valorados <= 3:
        print("recomendador: top9", file=sys.stdout)
        restaurant_ids = recomendar_top_9(user_id, interacciones)
    elif cant_valorados <= 5:
        print("recomendador: perfil", file=sys.stdout)
        #restaurant_ids = recomendar_perfil(user_id, interacciones)
        restaurant_ids = recomendar_perfil_v2(user_id, interacciones)
    else:
        print("recomendador: surprise", file=sys.stdout)
        restaurant_ids = recomendar_surprise(user_id, interacciones)
        #restaurant_ids = recomendar_lightfm(user_id, interacciones)
        #restaurant_ids = recomendar_lightfm_with_features(user_id, interacciones)
        #restaurant_ids = recomendar_whoosh(user_id, interacciones)

    # TODO: como completo las recomendaciones cuando vienen menos de 9?

    recomendaciones = utils.datos_restaurants(restaurant_ids)   

    return recomendaciones

