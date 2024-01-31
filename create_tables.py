import pandas as pd
import sqlite3

# Leer archivos CSV
df1 = pd.read_csv('/Users/vaguero/app/csv/restaurants_cafe_fixed.csv')
df2 = pd.read_csv('/Users/vaguero/app/csv/usuarios_cafe_fixed.csv')
df_interactions = pd.read_csv('/Users/vaguero/app/csv/interacciones_cafe_fixed.csv')

df_interactions = df_interactions.drop('Unnamed: 0', axis=1, errors='ignore')
print(df_interactions.shape)
df_interactions = df_interactions.drop_duplicates(subset=['restaurant_id', 'user_id'])
print(df_interactions.shape)

conn = sqlite3.connect('/Users/vaguero/app/data/database.db')

# Eliminar las tablas si existen
conn.execute("DROP TABLE IF EXISTS restaurants;")
conn.execute("DROP TABLE IF EXISTS users;")
conn.execute("DROP TABLE IF EXISTS interactions;")

# Crear tablas en la base de datos
create_table_restaurants = '''
CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id int PRIMARY KEY,
    name varchar(100),
    num_reviews int,
    latitude numeric,
    longitude numeric,
    timezone varchar(50), 
    location varchar(100),
    raw_ranking numeric,
    ranking_position int,
    ranking_caba int,
    rating numeric,
    price varchar(50),
    price_level varchar(10),
    phone varchar(20),
    website varchar(255),
    email varchar(100),
    address varchar(255),
    postalcode varchar(20),
    image_url varchar(255),
    type varchar(50),
    url varchar(255),
    special_diets varchar(100),
    califications varchar(255),
    traveler_rating_excelente int,
    traveler_rating_muy_bueno int,
    traveler_rating_regular int,
    traveler_rating_malo int,
    traveler_rating_horrible int,
    califications_comida numeric,
    califications_servicio numeric,
    califications_calidad_precio numeric,
    califications_ambiente numeric,
    display_hours varchar(255)
);

'''

create_table_users = '''
CREATE TABLE IF NOT EXISTS users (
    user_id varchar(50) PRIMARY KEY,
    nick varchar(50) UNIQUE,
    user varchar(50),
    locate varchar(50),
    num_opinions int,
    total_points int,
    helpful_counts int,
    critical_level int
);
'''

create_table_interactions = '''
CREATE TABLE IF NOT EXISTS interactions (
    restaurant_id bigint,
    user_id varchar(50),
    rating int,
    rating_date datetime,
    title varchar(50),
    opinion text,
    processed_opinion text,
    processed_title text,
    PRIMARY KEY (user_id, restaurant_id)
);
'''
create_table_interactions_train = '''
CREATE TABLE IF NOT EXISTS interactions_train (
    restaurant_id bigint,
    user_id varchar(50),
    rating int,
    rating_date datetime,
    title varchar(50),
    opinion text,
    processed_opinion text,
    processed_title text,
    PRIMARY KEY (user_id, restaurant_id)
);
'''

create_table_interactions_test = '''
CREATE TABLE IF NOT EXISTS interactions_test (
    restaurant_id bigint,
    user_id varchar(50),
    rating int,
    rating_date datetime,
    title varchar(50),
    opinion text,
    processed_opinion text,
    processed_title text,
    PRIMARY KEY (user_id, restaurant_id)
);
'''


# Ejecutar las consultas de creación de tablas
conn.execute(create_table_restaurants)
conn.execute(create_table_users)
conn.execute(create_table_interactions)
conn.execute(create_table_interactions_train)
conn.execute(create_table_interactions_test)

# Insertar datos en la base de datos
df1.to_sql('restaurants', conn, index=False, if_exists='append')
df2.to_sql('users', conn, index=False, if_exists='append')
df_interactions.to_sql('interactions', conn, index=False, if_exists='append')


# Cerrar la conexión
conn.close()