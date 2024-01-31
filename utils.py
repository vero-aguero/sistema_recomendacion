import sqlite3
import os
import uuid

THIS_FOLDER = os.path.dirname(os.path.abspath("__file__"))
#THIS_FOLDER = '/home/veroaguero/sistema_recomendacion/'

def sql_execute(query, params=None):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    cur = con.cursor()    
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)
    
    con.commit()
    con.close()
    return


def sql_one_select(query, params=None):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    cur = con.cursor()
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)
    ret = res.fetchone()  # Utilizamos fetchone para obtener un solo registro
    con.close()
    return ret[0]


def sql_select(query, params=None):
    con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
    con.row_factory = sqlite3.Row # esto es para que devuelva registros en el fetchall
    cur = con.cursor()    
    if params:
        res = cur.execute(query, params)
    else:
        res = cur.execute(query)
    ret = res.fetchall()
    con.close()
    return ret


def generar_user_id():
    return str(uuid.uuid4()).replace('-', '')


def crear_usuario(nick):
    user_id = generar_user_id()  
    query = "INSERT INTO users(user_id,nick,user,num_opinions,total_points,helpful_counts,critical_level) VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING;" # si el user_id existia, se produce un conflicto y le digo que no haga nada
    sql_execute(query, (user_id,nick,nick,0,0,0,0))
    return


def get_user_id(nick):
    query = "SELECT user_id FROM users WHERE nick = ?;"
    user_id = sql_one_select(query, (nick,))
    return user_id


def insertar_interacciones(restaurant_id, user_id, rating, titulo, review, interacciones="interactions"):
    query = f"INSERT INTO {interacciones}(restaurant_id, user_id, rating, title, opinion) VALUES (?, ?, ?, ?, ?) ON CONFLICT (restaurant_id, user_id) DO UPDATE SET rating=?;" # si el rating existia lo actualizo
    sql_execute(query, (restaurant_id, user_id, rating, titulo, review, rating))
    return


def reset_usuario(user_id, interacciones="interactions"):
    query = f"DELETE FROM {interacciones} WHERE user_id = ?;"
    sql_execute(query, (user_id,))
    return 


def obtener_restaurant(restaurant_id):
    query = "SELECT * FROM restaurants WHERE restaurant_id = ?;"
    restaurant = sql_select(query, (restaurant_id))[0]
    return restaurant


def valorados(user_id, interacciones="interactions"):
    query = f"SELECT * FROM {interacciones} WHERE user_id = ? AND rating > 0"
    valorados = sql_select(query, (user_id,))
    return valorados


def ignorados(user_id, interacciones="interactions"):
    query = f"SELECT * FROM {interacciones} WHERE user_id = ? AND rating = 0"
    ignorados = sql_select(query, (user_id,))
    return ignorados


def datos_restaurants(restaurant_id):
    query = f"SELECT DISTINCT * FROM restaurants WHERE restaurant_id IN ({','.join(['?']*len(restaurant_id))})"
    restaurants = sql_select(query, restaurant_id)
    return restaurants

