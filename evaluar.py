import sqlite3
import pandas as pd
import os
import math
from sklearn.metrics import ndcg_score, recall_score, mean_squared_error
import numpy as np

import recomendar
import utils

utils.sql_execute("DELETE FROM interactions_train")
utils.sql_execute("DELETE FROM interactions_test")
id_users = utils.sql_select("SELECT user_id FROM interactions GROUP BY user_id HAVING COUNT(*) >= 20")
for row_user in id_users:
    id_restaurants = utils.sql_select("SELECT * FROM interactions WHERE user_id = ?", (row_user["user_id"], ))
    cant_train = int(len(id_restaurants) * 0.8)
    for row in id_restaurants[0:cant_train]:
        utils.sql_execute("INSERT INTO interactions_train(restaurant_id, user_id, rating) VALUES (?, ?, ?)", [row["restaurant_id"], row_user["user_id"], row["rating"]])
    for row in id_restaurants[cant_train:]:    
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

def recall_at(ground_truth, recommendation, k=9):
    relevant_items = set(ground_truth)
    recommended_items = recommendation[:k]
    recall = len(relevant_items.intersection(recommended_items)) / len(relevant_items)
    return recall

def rmse(ground_truth, prediction):
    return np.sqrt(mean_squared_error(ground_truth, prediction))

users_id = utils.sql_select("SELECT DISTINCT user_id FROM interactions_test")

print("Evaluating model:")
for row in users_id:
    restaurants_watched = [row["restaurant_id"] for row in
                           utils.sql_select("SELECT DISTINCT restaurant_id FROM interactions_test WHERE user_id = ?",
                                            (row["user_id"],))]
    print("SQL")
    print(restaurants_watched)
    print(sorted(restaurants_watched))
    recommendation = recomendar.recomendar(row["user_id"], interacciones="interactions_test")
    restaurants_recommended = [row['restaurant_id'] for row in recommendation]
    print(sorted(restaurants_recommended))

    p_at_9 = precision_at(restaurants_watched, restaurants_recommended, n=9)
    ndcg_score = ndcg(restaurants_watched, restaurants_recommended)
    recall_at_9 = recall_at(restaurants_watched, restaurants_recommended, k=9)


    print(f"User {row['user_id']}\tPrecision@9: {p_at_9:.5f}\tNDCG@10: {ndcg_score:.5f}\tRecall@9: {recall_at_9:.5f}")





