"""
Flask API + Telegram Listener (combined)
- Flask runs in a background thread
- Telethon runs on the main asyncio event loop
- Both share the same asyncpg connection pool (NeonDB)

Requirements:
    pip install telethon asyncpg python-dotenv flask flask-cors sqlalchemy
"""

import asyncio
import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import cross_origin
from sqlalchemy import create_engine, pool, text
from telethon import TelegramClient, events
from telethon.tl.types import WebPage, WebPageEmpty
from pprint import pprint
from urllib.parse import urlparse, unquote, parse_qs
import requests

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("app")

# ── Config ────────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ["DATABASE_URL"]
# NEON_DSN     = os.environ["NEON_DSN"]

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("SESSION_NAME", "listener_session")
TARGET_GROUPS = ["@evstationcambodia", "Bak Tapea"]
PHONE = os.environ["PHONE"]

KEYWORDS = [
    "buy",
    "signal",
    "alert",
    # Add your keywords here
]

# ── SQLAlchemy Pool (Flask / sync routes) ─────────────────────────────────────

engine = create_engine(
    DATABASE_URL,
    poolclass=pool.QueuePool,
    pool_size=5,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)

# ── NeonDB SQL ────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO ev_stations (
    location_name, lat, lng,
    city, battery, charge_type,
    app, open, payment, map_link
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (lat, lng) DO NOTHING
"""

# ── asyncpg pool (shared by Telegram listener) ────────────────────────────────

_db_pool: Optional[asyncpg.Pool] = None


async def get_db_pool() -> asyncpg.Pool:
    global _db_pool
    if _db_pool is None:
        _db_pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=2, max_size=5, ssl=True
        )
    return _db_pool


# ── Flask App ─────────────────────────────────────────────────────────────────

app = Flask(__name__)


@app.route("/")
def index():
    return jsonify({"status": "ok"})


@app.route("/api/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"db": "connected", "pool": engine.pool.status()})
    except Exception as e:
        log.error("Health check failed: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations", methods=["GET"])
@cross_origin()
def get_stations():
    try:
        page = max(1, int(request.args.get("page", 1)))
        limit = min(int(request.args.get("limit", 1000)), 5000)
        offset = (page - 1) * limit
        search = request.args.get("search", "").strip()
        city = request.args.get("city", "").strip()

        filters, params = [], {"limit": limit, "offset": offset}

        if search:
            filters.append("(location_name ILIKE :search OR city ILIKE :search)")
            params["search"] = f"%{search}%"
        if city:
            filters.append("city ILIKE :city")
            params["city"] = f"%{city}%"

        where = ("WHERE " + " AND ".join(filters)) if filters else ""

        with engine.connect() as conn:
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM ev_stations {where}"), params
            ).scalar()

            rows = [
                dict(row._mapping)
                for row in conn.execute(
                    text(
                        f"SELECT * FROM ev_stations {where} ORDER BY id ASC LIMIT :limit OFFSET :offset"
                    ),
                    params,
                )
            ]

        return jsonify(
            {
                "data": rows,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "total_pages": -(-total // limit),
                    "has_next": page < -(-total // limit),
                    "has_prev": page > 1,
                },
            }
        )

    except ValueError:
        return jsonify({"error": "page and limit must be integers"}), 400
    except Exception as e:
        log.error("GET /api/stations error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations/<int:station_id>", methods=["GET"])
@cross_origin()
def get_station(station_id):
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM ev_stations WHERE id = :id"), {"id": station_id}
            ).fetchone()

        if not row:
            return jsonify({"error": "Station not found"}), 404

        return jsonify({"data": dict(row._mapping)})
    except Exception as e:
        log.error("GET /api/stations/%s error: %s", station_id, e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations", methods=["POST"])
@cross_origin()
def create_station():
    body = request.get_json(silent=True) or {}
    missing = [f for f in ("location_name", "lat", "lng") if not body.get(f)]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    INSERT INTO ev_stations
                        (location_name, lat, lng, city, battery, charge_type, app, open, payment, map_link)
                    VALUES
                        (:location_name, :lat, :lng, :city, :battery, :charge_type, :app, :open, :payment, :map_link)
                    ON CONFLICT ON CONSTRAINT unique_station_location DO UPDATE SET
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
            ).fetchone()

        return jsonify({"data": dict(row._mapping)}), 201
    except Exception as e:
        log.error("POST /api/stations error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations/<int:station_id>", methods=["DELETE"])
@cross_origin()
def delete_station(station_id):
    try:
        with engine.begin() as conn:
            deleted = conn.execute(
                text("DELETE FROM ev_stations WHERE id = :id RETURNING id"),
                {"id": station_id},
            ).fetchone()

        if not deleted:
            return jsonify({"error": "Station not found"}), 404

        return jsonify({"message": f"Station {station_id} deleted"})
    except Exception as e:
        log.error("DELETE /api/stations/%s error: %s", station_id, e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/no-lat-lng-stations", methods=["GET"])
@cross_origin()
def get_no_lat_lng_stations():
    try:
        page = max(1, int(request.args.get("page", 1)))
        limit = min(int(request.args.get("limit", 1000)), 5000)
        offset = (page - 1) * limit

        filters, params = [], {"limit": limit, "offset": offset}
        filters.append(
            "(lat IS NULL OR lat::text = '' OR lng IS NULL OR lng::text = '')"
        )
        where = ("WHERE " + " AND ".join(filters)) if filters else ""

        with engine.connect() as conn:
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM ev_stations {where}"), params
            ).scalar()

            rows = [
                dict(row._mapping)
                for row in conn.execute(
                    text(
                        f"SELECT * FROM ev_stations {where} ORDER BY id ASC LIMIT :limit OFFSET :offset"
                    ),
                    params,
                )
            ]

        return jsonify(
            {
                "data": rows,
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": total,
                    "total_pages": -(-total // limit),
                    "has_next": page < -(-total // limit),
                    "has_prev": page > 1,
                },
            }
        )

    except ValueError:
        return jsonify({"error": "page and limit must be integers"}), 400
    except Exception as e:
        log.error("GET /api/stations error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/stations/<int:station_id>", methods=["PUT"])
@cross_origin()
def update_station(station_id):
    body = request.get_json(silent=True) or {}
    if not body:
        return jsonify({"error": "Request body is empty"}), 400

    # Only update fields that were actually sent in the request
    UPDATABLE_FIELDS = [
        "location_name",
        "lat",
        "lng",
        "city",
        "battery",
        "charge_type",
        "app",
        "open",
        "payment",
        "map_link",
    ]
    updates = {f: body[f] for f in UPDATABLE_FIELDS if f in body}

    if not updates:
        return (
            jsonify(
                {
                    "error": f"No valid fields to update. Allowed: {', '.join(UPDATABLE_FIELDS)}"
                }
            ),
            400,
        )

    if "lat" in updates:
        updates["lat"] = float(updates["lat"])
    if "lng" in updates:
        updates["lng"] = float(updates["lng"])

    try:
        set_clause = ", ".join(f"{col} = :{col}" for col in updates)
        updates["station_id"] = station_id

        with engine.begin() as conn:
            row = conn.execute(
                text(
                    f"UPDATE ev_stations SET {set_clause} WHERE id = :station_id RETURNING *"
                ),
                updates,
            ).fetchone()

        if not row:
            return jsonify({"error": "Station not found"}), 404

        return jsonify({"data": dict(row._mapping)})
    except Exception as e:
        log.error("PUT /api/stations/%s error: %s", station_id, e)
        return jsonify({"error": str(e)}), 500


# ── Telegram Listener ─────────────────────────────────────────────────────────


def to_float(value):
    if value in ("", None):
        return None
    return float(value)


async def start_telegram_listener():
    db_pool = await get_db_pool()

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start(phone=PHONE)
    me = await client.get_me()
    log.info("Telegram: logged in as %s (@%s)", me.first_name, me.username)

    # Resolve group entities
    group_entities = []
    for g in TARGET_GROUPS:
        try:
            entity = await client.get_entity(int(g) if g.lstrip("-").isdigit() else g)
            group_entities.append(entity)
            log.info("Listening to group: %s", getattr(entity, "title", g))
        except Exception as e:
            log.warning("Could not resolve group '%s': %s", g, e)

    if not group_entities:
        log.error("No valid groups resolved — check TARGET_GROUPS in .env")
        return

    @client.on(events.NewMessage(chats=group_entities))
    async def on_new_message(event: events.NewMessage.Event):
        emoji_pattern = re.compile(r"⚡️|📍|🔋|🔌")
        pprint("New message received:")
        msg_text = event.message.message or ""
        print(event)
        matched_kw = emoji_pattern.search(msg_text)
        log.warning("dddddddd not resolve group '%s'", msg_text)
        if not matched_kw:
            return

        # pprint(message)
        lines = [l.strip() for l in event.message.text.split("\n") if l.strip()]

        data = {
            "location": "",
            "battery": "",
            "charge_type": "",
            "app": "",
            "open": "",
            "payment": "",
            "map_link": "",
            "display_url": "",
            "lat": "",
            "lang": "",
            "city": "",
        }

        for line in lines:
            if line.startswith("📍"):
                data["location"] = line.replace("📍", "").strip()
            elif line.startswith("🔋"):
                data["battery"] = line.replace("🔋", "").strip()
            elif line.startswith("🔌"):
                data["charge_type"] = line.replace("🔌", "").strip()
            elif line.startswith("📱"):
                data["app"] = line.replace("📱", "").replace("App", "").strip()
            elif line.startswith("⏰"):
                data["open"] = line.replace("⏰", "").strip()
            elif line.startswith("💵"):
                data["payment"] = line.replace("💵", "").strip()
            elif line.startswith("http"):
                data["map_link"] = line

        if event.message.media and hasattr(event.message.media, "webpage"):
            webpage = event.message.media.webpage

            if webpage and not isinstance(webpage, WebPageEmpty):
                data["display_url"] = webpage.display_url
                print("Display URL:", webpage.display_url)
                print("................")

                # parsed_url = urlparse(webpage.display_url)
                # qs = parse_qs(parsed_url.query)
                address = extract_address_or_coords(webpage.display_url)

                print(f"Extracted address/coords: {address}")
                print("................")
                params = {
                    "q": address,
                    "format": "json",
                    "addressdetails": 1,
                    "accept-language": "en",
                    "limit": 5,
                }

                if address is not None:
                    response = requests.get(
                        "https://nominatim.openstreetmap.org/search",
                        params=params,
                        headers={"User-Agent": "python-script"},
                    )
                    datas = response.json()

                    if datas:
                        # city = datas[0]["address"].get("city")
                        pprint(datas)
                        data["city"] = datas[0]["address"].get("city") or datas[0][
                            "address"
                        ].get("state")
                        data["lat"] = datas[0]["lat"]
                        data["lng"] = datas[0]["lon"]
                    else:
                        print("Address not found")
                        # place = google_geocode(address)
                        # print(place)
                        # print("-------- ======== ----------")
                else:
                    print("Could not extract address or coordinates from URL")

        pprint(data)
        async with db_pool.acquire() as conn:
            await conn.execute(
                INSERT_SQL,
                data.get("location"),
                to_float(data.get("lat")),
                to_float(data.get("lng")),
                data.get("city"),
                data.get("battery"),
                data.get("charge_type"),
                data.get("app"),
                data.get("open"),
                data.get("payment"),
                data.get("map_link"),
            )

        log.info(
            "Matched '%s' in %s from @%s: %s",
            matched_kw,
            data.get("location"),
            data.get("city"),
            data.get("battery"),
        )

    log.info("Telegram listener active. Waiting for messages...")
    await client.run_until_disconnected()


def extract_address_or_coords(url: str) -> str:
    """
    Extracts a human-readable address or coordinates from Google Maps URLs.
    Supports:
      - /maps?q=...        (search query)
      - /maps/place/...    (place URL)
      - /maps/@lat,lng     (coordinates)
      - /maps/search/lat,lng (coordinates)
    Returns:
      - Clean address string OR "lat,lng" string.
    """
    parsed = urlparse(url)
    path = parsed.path
    query = parsed.query

    # 1️⃣ Handle ?q= search URLs
    if query:
        qs = parse_qs(query)
        if "q" in qs:
            address = qs["q"][0]
            print("11111")
            print(address)
            # decode URL-encoded characters and replace '+' with space
            return unquote(address).replace("+", " ")

    # 2️⃣ Handle /place/... URLs
    if "/place/" in path:
        place_part = path.split("/place/")[1].split("/")[0]
        print("2222222")
        return unquote(place_part).replace("+", " ")

    # 3️⃣ Handle /@lat,lng URLs
    if "/@" in path:
        print("33333")
        try:
            coords = path.split("/@")[1].split(",")[:2]
            return f"{coords[0]},{coords[1]}"
        except IndexError:
            return None

    # 4️⃣ Handle /search/lat,lng URLs
    if "/search/" in path:
        print("44444")
        try:
            coords = path.split("/search/")[1].split("?")[0].split(",")[:2]
            return f"{coords[0]},{coords[1]}"
        except IndexError:
            return None

    # Nothing matched
    return None


# ── Entrypoint ────────────────────────────────────────────────────────────────


def run_flask():
    """Run Flask in a background daemon thread."""
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)


async def main():
    # Start Flask in a background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    log.info("Flask API started on http://0.0.0.0:5000")

    # Run Telegram listener on the main event loop
    try:
        await start_telegram_listener()
    finally:
        if _db_pool:
            await _db_pool.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
