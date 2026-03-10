from flask import Flask, request, jsonify
import psycopg2
import os
from flask_cors import cross_origin
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"))


app = Flask(__name__)


@app.route("/")
def index():
    return jsonify({"status": "ok"})


# Create table
@app.route("/init-db")
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS ev_stations (
        id SERIAL PRIMARY KEY,
        location_name TEXT,
        lat DOUBLE PRECISION,
        lng DOUBLE PRECISION,
        city TEXT,
        battery TEXT,
        charge_type TEXT,
        app TEXT,
        open TEXT,
        payment TEXT,
        map_link TEXT
    );
    """
    )

    cur.execute(
        """
    CREATE UNIQUE INDEX unique_station_location
ON ev_stations (lat, lng)
WHERE lat IS NOT NULL AND lng IS NOT NULL;
    """
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "db initialized"}


@app.route("/stations", methods=["POST"])
def create_station():
    data = request.json

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
    INSERT INTO ev_stations (location_name, lat, lng, city, battery, charge_type, app, open, payment, map_link)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (lat, lng) DO NOTHING
    """,
        (
            data.get("location_name"),
            (data.get("lat")),
            (data.get("lng")),
            data.get("city"),
            data.get("battery"),
            data.get("charge_type"),
            data.get("app"),
            data.get("open"),
            data.get("payment"),
            data.get("map_link"),
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "inserted"}


@app.route("/stations")
@cross_origin()
def get_stations():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT id,location_name,lat,lng,city,battery,charge_type,app,open,payment,map_link FROM ev_stations"
    )

    rows = cur.fetchall()

    result = []
    for r in rows:
        result.append(
            {
                "id": r[0],
                "location_name": r[1],
                "lat": r[2],
                "lng": r[3],
                "city": r[4],
                "battery": r[5],
                "charge_type": r[6],
                "app": r[7],
                "open": r[8],
                "payment": r[9],
                "map_link": r[10],
            }
        )

    cur.close()
    conn.close()

    return jsonify(result)


# if __name__ == "__main__":
#     app.run(debug=True)

# required export
# handler = app
