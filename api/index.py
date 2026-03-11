from flask import Flask, request, jsonify
from flask_cors import cross_origin
from sqlalchemy import create_engine, text, pool
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

# ── Connection Pool ───────────────────────────────────────────
# QueuePool is SQLAlchemy's default — thread-safe, auto-recycles
DATABASE_URL = os.environ.get("DATABASE_URL", "")

engine = create_engine(
    DATABASE_URL,
    poolclass=pool.QueuePool,
    pool_size=5,  # max persistent connections
    max_overflow=5,  # extra connections when pool is full (total max: 10)
    pool_timeout=30,  # seconds to wait for a connection before error
    pool_recycle=1800,  # recycle connections every 30min (prevents stale conn)
    pool_pre_ping=True,  # test connection before using (auto-reconnect)
)


# ── Routes ────────────────────────────────────────────────────


@app.route("/")
def index():
    return jsonify({"status": "ok"})


@app.route("/api/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        pool_status = engine.pool.status()
        return jsonify(
            {
                "db": "connected",
                "pool": pool_status,  # shows checked-in/out connection counts
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations", methods=["GET"])
@cross_origin()
def get_stations():
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 1000))
        offset = (page - 1) * limit

        # ── Search params ─────────────────────────────────────
        search = request.args.get("search", "").strip()  # searches both fields
        city = request.args.get("city", "").strip()  # filter by city only

        # ── Build WHERE clause ────────────────────────────────
        filters = []
        params = {"limit": limit, "offset": offset}

        if search:
            filters.append(
                """
                (location_name ILIKE :search OR city ILIKE :search)
            """
            )
            params["search"] = f"%{search}%"

        if city:
            filters.append("city ILIKE :city")
            params["city"] = f"%{city}%"

        where = "WHERE " + " AND ".join(filters) if filters else ""

        with engine.connect() as conn:
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM ev_stations {where}"),
                params,
            ).scalar()

            result = conn.execute(
                text(
                    f"""
                    SELECT *
                    FROM   ev_stations
                    {where}
                    ORDER  BY id ASC
                    LIMIT  :limit
                    OFFSET :offset
                """
                ),
                params,
            )
            rows = [dict(row._mapping) for row in result]

        total_pages = -(-total // limit)

        return jsonify(
            {
                "data": rows,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1,
                },
            }
        )

    except ValueError:
        return jsonify({"error": "page and limit must be numbers"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations/<int:station_id>", methods=["GET"])
@cross_origin()
def get_station(station_id):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM ev_stations WHERE id = :id"),
                {"id": station_id},
            )
            row = result.fetchone()

        if not row:
            return jsonify({"error": "Station not found"}), 404

        return jsonify({"data": dict(row._mapping)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations", methods=["POST"])
@cross_origin()
def create_station():
    body = request.get_json()
    required = ["location_name", "lat", "lng"]
    for field in required:
        if not body.get(field):
            return jsonify({"error": f"{field} is required"}), 400

    try:
        with engine.begin() as conn:  # begin() auto-commits on exit
            result = conn.execute(
                text(
                    """
                    INSERT INTO ev_stations
                        (location_name, lat, lng, city, battery, charge_type, app, open, payment, map_link)
                    VALUES
                        (:location_name, :lat, :lng, :city, :battery, :charge_type, :app, :open, :payment, :map_link)
                    ON CONFLICT ON CONSTRAINT unique_station_location
                    DO UPDATE SET
                        location_name = EXCLUDED.location_name,
                        city          = EXCLUDED.city,
                        battery       = EXCLUDED.battery,
                        charge_type   = EXCLUDED.charge_type,
                        app           = EXCLUDED.app,
                        open          = EXCLUDED.open,
                        payment       = EXCLUDED.payment,
                        map_link      = EXCLUDED.map_link
                    RETURNING *
                """
                ),
                {
                    "location_name": body["location_name"],
                    "lat": float(body["lat"]),
                    "lng": float(body["lng"]),
                    "city": body.get("city"),
                    "battery": body.get("battery"),
                    "charge_type": body.get("charge_type"),
                    "app": body.get("app"),
                    "open": body.get("open"),
                    "payment": body.get("payment"),
                    "map_link": body.get("map_link"),
                },
            )
            row = result.fetchone()

        return jsonify({"data": dict(row._mapping)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations/<int:station_id>", methods=["DELETE"])
@cross_origin()
def delete_station(station_id):
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM ev_stations WHERE id = :id RETURNING id"),
                {"id": station_id},
            )
            if not result.fetchone():
                return jsonify({"error": "Station not found"}), 404

        return jsonify({"message": f"Station {station_id} deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# # ── WSGI entry for Vercel ─────────────────────────────────────
# def handler(environ, start_response):
#     return app.wsgi_app(environ, start_response)


if __name__ == "__main__":
    app.run(debug=True)
