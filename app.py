from flask import Flask, render_template, request, redirect, url_for, session, jsonify
import pymysql
import hashlib
from broker import EventBroker

app = Flask(__name__)
app.secret_key = "clave_secreta_123"

broker = EventBroker()
mensajes_en_memoria = {}

def get_db():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="",
        database="event_broker_db",
        charset="utf8mb4"
    )

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

@app.route("/")
def index():
    if "usuario" in session:
        return redirect(url_for("chat"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form["username"]
        password = hash_password(request.form["password"])
        db = get_db()
        cursor = db.cursor()
        cursor.execute("SELECT * FROM usuarios WHERE username=%s AND password=%s", (username, password))
        usuario = cursor.fetchone()
        db.close()
        if usuario:
            session["usuario"] = username
            return redirect(url_for("chat"))
        else:
            error = "Usuario o contraseña incorrectos"
    return render_template("login.html", error=error)

@app.route("/register", methods=["GET", "POST"])
def register():
    error = None
    if request.method == "POST":
        username = request.form["username"]
        password = hash_password(request.form["password"])
        db = get_db()
        cursor = db.cursor()
        try:
            cursor.execute("INSERT INTO usuarios (username, password) VALUES (%s, %s)", (username, password))
            db.commit()
            db.close()
            return redirect(url_for("login"))
        except:
            error = "Ese usuario ya existe"
            db.close()
    return render_template("login.html", error=error, registro=True)

@app.route("/chat")
def chat():
    if "usuario" not in session:
        return redirect(url_for("login"))
    return render_template("chat.html", usuario=session["usuario"])

@app.route("/send", methods=["POST"])
def send():
    if "usuario" not in session:
        return jsonify({"error": "no autenticado"}), 401
    data = request.json
    destinatario = data["destinatario"]
    contenido = data["contenido"]
    remitente = session["usuario"]
    canal = "-".join(sorted([remitente, destinatario]))
    mensaje = {"remitente": remitente, "contenido": contenido}
    def guardar(msg):
        db = get_db()
        cursor = db.cursor()
        cursor.execute(
            "INSERT INTO mensajes (remitente, destinatario, contenido, lamport_clock) VALUES (%s, %s, %s, %s)",
            (remitente, destinatario, msg["contenido"], msg["lamport"])
        )
        db.commit()
        db.close()
        clave = canal
        if clave not in mensajes_en_memoria:
            mensajes_en_memoria[clave] = []
        mensajes_en_memoria[clave].append(msg)
    broker.subscribe(canal, guardar) if canal not in broker.canales else None
    broker.publish(canal, mensaje)
    return jsonify({"ok": True, "lamport": mensaje["lamport"]})

@app.route("/messages/<destinatario>")
def messages(destinatario):
    if "usuario" not in session:
        return jsonify({"error": "no autenticado"}), 401
    remitente = session["usuario"]
    canal = "-".join(sorted([remitente, destinatario]))
    db = get_db()
    cursor = db.cursor()
    cursor.execute(
        "SELECT remitente, contenido, lamport_clock FROM mensajes WHERE (remitente=%s AND destinatario=%s) OR (remitente=%s AND destinatario=%s) ORDER BY lamport_clock ASC",
        (remitente, destinatario, destinatario, remitente)
    )
    filas = cursor.fetchall()
    db.close()
    resultado = [{"remitente": f[0], "contenido": f[1], "lamport": f[2]} for f in filas]
    return jsonify(resultado)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

if __name__ == "__main__":
    app.run(debug=True)