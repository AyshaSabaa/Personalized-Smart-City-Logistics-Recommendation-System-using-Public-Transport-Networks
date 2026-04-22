import sqlite3
from functools import wraps

from flask import Flask, g, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from logic import get_logic_service

app = Flask(__name__)
app.config["SECRET_KEY"] = "change-this-secret"
DATASET_PATH = "data/bus_data.xlsx"
DATABASE_PATH = "users.db"


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(error: Exception | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL
        )
        """
    )
    db.commit()


@app.before_request
def ensure_users_table() -> None:
    init_db()


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped_view


@app.route("/", methods=["GET", "POST"])
@app.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("home"))

    error = ""
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "").strip()
        if not email or not password:
            error = "Please enter email and password."
        else:
            user = get_db().execute(
                "SELECT * FROM users WHERE email = ?", (email,)
            ).fetchone()
            if user and check_password_hash(user["password"], password):
                session.clear()
                session["user_id"] = user["id"]
                session["username"] = user["username"]
                return redirect(url_for("home"))
            error = "Invalid email or password."

    return render_template("login.html", error=error)


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if "user_id" in session:
        return redirect(url_for("home"))

    error = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "").strip()

        if not username or not email or not password:
            error = "All fields are required."
        else:
            db = get_db()
            existing_user = db.execute(
                "SELECT id FROM users WHERE username = ? OR email = ?",
                (username, email),
            ).fetchone()
            if existing_user:
                error = "Username or email already exists."
            else:
                hashed_password = generate_password_hash(password)
                db.execute(
                    "INSERT INTO users (username, email, password) VALUES (?, ?, ?)",
                    (username, email, hashed_password),
                )
                db.commit()
                return redirect(url_for("login"))

    return render_template("signup.html", error=error)


@app.route("/home", methods=["GET", "POST"])
@login_required
def home():
    results = []
    search_data = {
        "source": "",
        "destination": "",
        "parcel_type": "",
    }

    error = ""

    if request.method == "POST":
        search_data["source"] = request.form.get("source", "").strip()
        search_data["destination"] = request.form.get("destination", "").strip()
        search_data["parcel_type"] = request.form.get("parcel_type", "").strip()

        if search_data["source"] and search_data["destination"]:
            try:
                logic_service = get_logic_service(DATASET_PATH)
                # Returns top 3 route options as list[dict].
                results = logic_service.recommend_parcel(
                    source=search_data["source"],
                    destination=search_data["destination"],
                    parcel_type=search_data["parcel_type"],
                    limit=3,
                )
            except FileNotFoundError:
                error = (
                    "Dataset not found. Add your Excel file at "
                    "'data/bus_data.xlsx' and try again."
                )

    return render_template(
        "home.html",
        results=results,
        search_data=search_data,
        error=error,
        username=session.get("username", ""),
    )


@app.route("/booking", methods=["GET", "POST"])
@login_required
def booking():
    selected = {
        "route_name": request.values.get("route_name", ""),
        "operator": request.values.get("operator", ""),
        "pickup_stop": request.values.get("pickup_stop", ""),
        "drop_stop": request.values.get("drop_stop", ""),
        "departure": request.values.get("departure", ""),
        "duration_hrs": request.values.get("duration_hrs", ""),
        "estimated_fare": request.values.get("estimated_fare", ""),
        "distance_km": request.values.get("distance_km", ""),
        "route_type": request.values.get("route_type", ""),
    }

    if request.method == "POST":
        # Placeholder: payment can be integrated here.
        payment_method = request.form.get("payment_method", "").strip()
        if payment_method and selected["route_name"]:
            return render_template(
                "booking.html",
                selected=selected,
                confirmed=True,
                username=session.get("username", ""),
            )

    return render_template(
        "booking.html",
        selected=selected,
        confirmed=False,
        username=session.get("username", ""),
    )


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    app.run(debug=True)
