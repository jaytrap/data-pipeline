"""
USGS earthquake transform.

Reads raw GeoJSON earthquake feeds from MinIO, extracts individual
quake events, and loads into fact_earthquake with location dimensions.

Raw USGS payload is a GeoJSON FeatureCollection:
{
    "type": "FeatureCollection",
    "features": [
        {
            "properties": {
                "mag": 4.2,
                "place": "10km NW of Cobb, CA",
                "time": 1694100000000,
                "type": "earthquake",
                "magType": "ml",
                "felt": 12,
                "tsunami": 0,
                "alert": "green"
            },
            "geometry": {
                "coordinates": [-122.8, 38.8, 2.5]
            },
            "id": "nc12345678"
        }
    ]
}

Deduplication uses the USGS event ID (unique per earthquake).
"""

import logging
from datetime import datetime, timezone

from transforms.utils import (
    get_db_connection,
    get_watermark,
    update_watermark,
    list_new_raw_files,
    read_raw_file,
    ensure_time_dimension,
    log_audit,
)

logger = logging.getLogger(__name__)

SOURCE_NAME = "usgs"
SOURCE_PATH = "usgs/earthquakes/"


def run(dag_id: str = "", run_id: str = "", **kwargs):
    """Main entry point called by Airflow."""

    log_audit(dag_id, "transform_usgs", run_id, "started")

    watermark = get_watermark(SOURCE_NAME)
    new_files = list_new_raw_files(SOURCE_PATH, since=watermark)

    if not new_files:
        logger.info("No new USGS files to process")
        log_audit(dag_id, "transform_usgs", run_id, "success",
                  details={"message": "no new files"})
        return

    records_in = 0
    records_out = 0
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            for file_path in new_files:
                payloads = read_raw_file(file_path)

                for payload in payloads:
                    if not isinstance(payload, dict):
                        continue

                    features = payload.get("features", [])

                    for feature in features:
                        props = feature.get("properties", {})
                        geom = feature.get("geometry", {})
                        event_id = feature.get("id")

                        if not event_id or not props.get("mag"):
                            continue

                        records_in += 1

                        # Parse coordinates
                        coords = geom.get("coordinates", [0, 0, 0])
                        longitude = coords[0] if len(coords) > 0 else None
                        latitude = coords[1] if len(coords) > 1 else None
                        depth = coords[2] if len(coords) > 2 else None

                        # Parse event time
                        event_time_ms = props.get("time", 0)
                        event_time = datetime.fromtimestamp(
                            event_time_ms / 1000, tz=timezone.utc
                        )

                        # Determine region from place string
                        place = props.get("place", "Unknown")
                        region = _extract_region(place)

                        # Upsert location dimension
                        location_id = _ensure_location(
                            cur, place, latitude, longitude, depth, region
                        )

                        time_id = ensure_time_dimension(event_time)

                        # Insert fact (skip duplicates by usgs_event_id)
                        cur.execute(
                            """
                            INSERT INTO fact_earthquake
                                (usgs_event_id, location_id, time_id,
                                 magnitude, mag_type, felt_reports,
                                 tsunami_flag, alert_level, ingested_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (usgs_event_id) DO NOTHING
                            """,
                            (
                                event_id, location_id, time_id,
                                props.get("mag"),
                                props.get("magType"),
                                props.get("felt"),
                                bool(props.get("tsunami", 0)),
                                props.get("alert"),
                            ),
                        )
                        if cur.rowcount > 0:
                            records_out += 1

        conn.commit()
    finally:
        conn.close()

    update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))

    log_audit(dag_id, "transform_usgs", run_id, "success",
              records_in=records_in, records_out=records_out)
    logger.info(f"USGS transform complete: {records_in} events "
                f"-> {records_out} new quakes loaded")


def _ensure_location(cur, place, lat, lon, depth, region) -> int:
    """Upsert a location and return its ID."""
    # Check if we already have this exact place
    cur.execute(
        "SELECT location_id FROM dim_location WHERE place = %s",
        (place,),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        """
        INSERT INTO dim_location (place, latitude, longitude, depth_km, region)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING location_id
        """,
        (place, lat, lon, depth, region),
    )
    return cur.fetchone()[0]


def _extract_region(place: str) -> str:
    """
    Extract a rough region from USGS place strings.
    e.g. '10km NW of Cobb, CA' -> 'CA'
         '45km S of Tanaga Volcano, Alaska' -> 'Alaska'
    """
    if not place:
        return "Unknown"

    # USGS format is usually "Xkm DIR of Place, Region"
    parts = place.split(",")
    if len(parts) >= 2:
        return parts[-1].strip()

    return place
