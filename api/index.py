from flask import Flask, request, jsonify
import os
from flask_cors import cross_origin
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()

app = Flask(__name__)

# ── Neon DB ───────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


@app.route("/")
def index():
    return jsonify({"status": "ok"})


@app.route("/api/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"db": "connected"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations", methods=["GET"])
@cross_origin()
def get_stations():
    try:
        # ── Pagination params ─────────────────────────────────
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 1000))
        offset = (page - 1) * limit

        with engine.connect() as conn:
            # Total count
            count_result = conn.execute(text("SELECT COUNT(*) FROM ev_stations"))
            total = count_result.scalar()

            # Paginated rows
            result = conn.execute(
                text(
                    """
                SELECT *
                FROM   ev_stations
                LIMIT  :limit
                OFFSET :offset
            """
                ),
                {"limit": limit, "offset": offset},
            )

            rows = [dict(row._mapping) for row in result]

        return jsonify(
            {
                "data": rows,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "total_pages": -(-total // limit),  # ceiling division
                    "has_next": page < -(-total // limit),
                    "has_prev": page > 1,
                },
            }
        )

    except ValueError:
        return jsonify({"error": "page and limit must be numbers"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Create table
# @app.route("/init-db")
# def init_db():
#     conn = get_conn()
#     cur = conn.cursor()

#     cur.execute(
#         """
#     CREATE TABLE IF NOT EXISTS ev_stations (
#         id SERIAL PRIMARY KEY,
#         location_name TEXT,
#         lat DOUBLE PRECISION,
#         lng DOUBLE PRECISION,
#         city TEXT,
#         battery TEXT,
#         charge_type TEXT,
#         app TEXT,
#         open TEXT,
#         payment TEXT,
#         map_link TEXT
#     );
#     """
#     )

#     cur.execute(
#         """
#     CREATE UNIQUE INDEX unique_station_location
# ON ev_stations (lat, lng)
# WHERE lat IS NOT NULL AND lng IS NOT NULL;
#     """
#     )

#     conn.commit()
#     cur.close()
#     conn.close()

#     return {"status": "db initialized"}


# @app.route("/stations", methods=["POST"])
# def create_station():
#     data = request.json

#     conn = get_conn()
#     cur = conn.cursor()

#     cur.execute(
#         """
#     INSERT INTO ev_stations (location_name, lat, lng, city, battery, charge_type, app, open, payment, map_link)
#     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#     ON CONFLICT (lat, lng) DO NOTHING
#     """,
#         (
#             data.get("location_name"),
#             (data.get("lat")),
#             (data.get("lng")),
#             data.get("city"),
#             data.get("battery"),
#             data.get("charge_type"),
#             data.get("app"),
#             data.get("open"),
#             data.get("payment"),
#             data.get("map_link"),
#         ),
#     )

#     conn.commit()
#     cur.close()
#     conn.close()

#     return {"status": "inserted"}


# @app.route("/stations")
# @cross_origin()
# def get_stations():
#     conn = get_conn()
#     cur = conn.cursor()

#     cur.execute(
#         "SELECT id,location_name,lat,lng,city,battery,charge_type,app,open,payment,map_link FROM ev_stations"
#     )

#     rows = cur.fetchall()

#     result = []
#     for r in rows:
#         result.append(
#             {
#                 "id": r[0],
#                 "location_name": r[1],
#                 "lat": r[2],
#                 "lng": r[3],
#                 "city": r[4],
#                 "battery": r[5],
#                 "charge_type": r[6],
#                 "app": r[7],
#                 "open": r[8],
#                 "payment": r[9],
#                 "map_link": r[10],
#             }
#         )

#     cur.close()
#     conn.close()

#     return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)

# required export
# handler = app
