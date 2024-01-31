import sqlite3
import pandas as pd
import os
import math

import recomendar
import utils

utils.sql_execute("DELETE FROM interactions_train")
#utils.sql_execute("INSERT INTO interactions_train SELECT * FROM interactions")
utils.sql_execute("DELETE FROM interactions_test")
id_users = utils.sql_select("SELECT user_id FROM interactions GROUP BY user_id HAVING COUNT(*) >= 20")
print(len(id_users))
for row_user in id_users:
    id_restaurants = utils.sql_select("SELECT * FROM interactions WHERE user_id = ?", (row_user["user_id"], ))
    print(len(id_restaurants))
    cant_train = int(len(id_restaurants) * 0.8)
    print(cant_train)
    for row in id_restaurants[0:cant_train]:
        utils.sql_execute("INSERT INTO interactions_train(restaurant_id, user_id, rating) VALUES (?, ?, ?)", [row["restaurant_id"], row_user["user_id"], row["rating"]])
    for row in id_restaurants[cant_train:]:    
        #utils.sql_execute("INSERT INTO interactions_test(restaurant_id, user_id, rating) VALUES (?, ?, ?)", [row["restaurant_id"], row_user["user_id"], row["rating"]])
    
        utils.sql_execute("INSERT INTO interactions_test SELECT * FROM interactions WHERE user_id = ? AND restaurant_id = ?", [row_user["user_id"], row["restaurant_id"]])
        utils.sql_execute("DELETE FROM interactions_train WHERE user_id = ? AND restaurant_id = ?", [row_user["user_id"], row["restaurant_id"]])
    print(row["user_id"])

def ndcg(groud_truth, recommendation):
    dcg = 0
    idcg = 0
    for i, r in enumerate(recommendation):
        rel = int(r in groud_truth)
        dcg += rel / math.log2(i+1+1)
        idcg += 1 / math.log2(i+1+1)

    return dcg / idcg

def precision_at(ground_truth, recommendation, n=9):
    return len(set(ground_truth[:n-1]).intersection(recommendation[:len(ground_truth[:n-1])])) / len(ground_truth[:n-1])

users_id = utils.sql_select("SELECT DISTINCT user_id FROM interactions_test")

print(users_id)
for row in users_id:
    libros_leidos = [row["restaurant_id"] for row in utils.sql_select("SELECT DISTINCT restaurant_id FROM interactions_test WHERE user_id = ?", (row["user_id"],))]
    recomendacion = recomendar.recomendar(row["user_id"], interacciones="interactions_test")
    p = precision_at(libros_leidos, recomendacion)
    n = ndcg(libros_leidos, recomendacion)
    print(f"{row['user_id']}\t\tndcg: {n:.5f}\tprecision@9: {p: .5f}")

