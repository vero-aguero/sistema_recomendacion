#!/usr/bin/env python3
from flask import Flask, render_template, request, make_response, redirect, url_for
import sqlite3
import os
import recomendar
import utils
import sys

app = Flask(__name__)

#@app.route('/', methods=('GET', 'POST'))
#def login():
#    if request.method == 'POST' and 'user_id' in request.form:
#        user_id = request.form['user_id']
#        THIS_FOLDER='/Users/vaguero/app/'
#        con = sqlite3.connect(os.path.join(THIS_FOLDER, "data/database.db"))
#        cur = con.cursor()
#        query = """
#            INSERT INTO users(user_id)
#            VALUES (?)
#            ON CONFLICT DO NOTHING
#        """
#        res = cur.execute(query, (user_id,))
#        con.commit()
#        con.close()

#        res = make_response(redirect("/recomendaciones"))
#        res.set_cookie('user_id', user_id)
#        return res

#    if request.method == 'GET' and 'user_id' in request.cookies:
#        res = make_response(redirect("/recomendaciones"))
#        return res

#    return render_template('login.html')

#@app.route("/recomendaciones")
#def recomendaciones():
#    page = int(request.args.get('page', 1))

#    con = sqlite3.connect("data/database.db")
#    con.row_factory = sqlite3.Row
#    cur = con.cursor()
#    sql = "SELECT * FROM restaurants"
#    res = cur.execute(sql)
#    restaurants = res.fetchall()   
#    con.close()
    
#    return render_template('index.html', restaurants=restaurants, current_page=page)

@app.route('/', methods=('GET', 'POST'))
def login():
    # si me mandaron el formulario y tiene user_id... 

    if request.method == 'POST' and 'nick' in request.form:
        nick = request.form['nick']

        # creo el usuario al insertar el user_id en la tabla "lectores"
        utils.crear_usuario(nick)

        # mando al usuario a la página de recomendaciones
        res = make_response(redirect("/recomendaciones"))

        # pongo el user_id en una cookie para recordarlo
        res.set_cookie('nick', nick)
        return res

    # si alguien entra a la página principal y conozco el usuario
    if request.method == 'GET' and 'nick' in request.cookies:
        return make_response(redirect("/recomendaciones"))

    # sino, le muestro el formulario de login
    return render_template('login.html')


@app.route('/recomendaciones', methods=('GET', 'POST'))
def recomendaciones():
    nick = request.cookies.get('nick')
    user_id = str(utils.get_user_id(nick))

    restaurants = recomendar.recomendar(user_id)

    for restaurant in restaurants:
        utils.insertar_interacciones(restaurant["restaurant_id"], user_id, 0,'','')

    cant_valorados = len(utils.valorados(user_id))
    cant_ignorados = len(utils.ignorados(user_id))
    
    return render_template("recomendaciones.html", restaurants=restaurants, user_id=user_id, nick=nick, cant_valorados=cant_valorados, cant_ignorados=cant_ignorados)


@app.route('/guardar_review', methods=['POST'])
def guardar_review():
    nick = request.cookies.get('nick')
    user_id = str(utils.get_user_id(nick))

    restaurant_id = request.form.get('restaurant_id')
    titulo = request.form.get('titulo')
    review = request.form.get('review')
    puntuacion = request.form.get('puntuacion')

    utils.insertar_interacciones(restaurant_id, user_id, puntuacion, titulo, review)

    return redirect(url_for('recomendaciones', restaurant_id=restaurant_id))


@app.route('/reset')
def reset():
    nick = request.cookies.get('nick')
    user_id = str(utils.get_user_id(nick))
    utils.reset_usuario(user_id)

    return make_response(redirect("/recomendaciones"))


@app.route("/detalle/<restaurant_id>")
def libro(restaurant_id):
    con = sqlite3.connect("data/database.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    sql = "SELECT * FROM restaurants WHERE restaurant_id = ?"
    res = cur.execute(sql, (restaurant_id,))
    restaurant = res.fetchall()[0]
    con.close()

    return render_template("detalle.html", restaurant=restaurant)


if __name__ == "__main__":
    app.run(debug=True)

    