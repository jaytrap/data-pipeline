"""
NASA Near-Earth Object transform.

Reads raw NEO feed from MinIO and loads asteroid approach data.

Raw NASA NEO payload:
{
    "near_earth_objects": {
        "2026-09-07": [
            {
                "id": "12345",
                "name": "Asteroid Name",
                "absolute_magnitude_h": 22.5,
                "estimated_diameter": {
                    "kilometers": {"min": 0.05, "max": 0.12}
                },
                "is_potentially_hazardous_asteroid": false,
                "close_approach_data": [{
                    "close_approach_date": "2026-09-07",
                    "relative_velocity": {"kilometers_per_hour": "45000"},
                    "miss_distance": {"kilometers": "5000000"},
                    "orbiting_body": "Earth"
                }]
            }
        ]
    }
}
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

SOURCE_NAME = "nasa"
SOURCE_PATH = "nasa/"


def run(dag_id: str = "", run_id: str = "", **kwargs):
    log_audit(dag_id, "transform_nasa", run_id, "started")

    watermark = get_watermark(SOURCE_NAME)
    new_files = list_new_raw_files(SOURCE_PATH, since=watermark)

    if not new_files:
        logger.info("No new NASA files to process")
        log_audit(dag_id, "transform_nasa", run_id, "success",
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

                    neo_objects = payload.get("near_earth_objects", {})

                    for date_str, asteroids in neo_objects.items():
                        for asteroid in asteroids:
                            records_in += 1

                            neo_id = asteroid.get("id", "")
                            neo_name = asteroid.get("name", "")
                            magnitude = _safe_float(
                                asteroid.get("absolute_magnitude_h")
                            )

                            # Diameter
                            diameter = asteroid.get(
                                "estimated_diameter", {}
                            ).get("kilometers", {})
                            diam_min = _safe_float(diameter.get("min"))
                            diam_max = _safe_float(diameter.get("max"))

                            is_hazardous = asteroid.get(
                                "is_potentially_hazardous_asteroid", False
                            )

                            # Process each close approach
                            approaches = asteroid.get(
                                "close_approach_data", []
                            )
                            for approach in approaches:
                                approach_date = approach.get(
                                    "close_approach_date"
                                )
                                velocity = _safe_float(
                                    approach.get(
                                        "relative_velocity", {}
                                    ).get("kilometers_per_hour")
                                )
                                miss_km = _safe_float(
                                    approach.get(
                                        "miss_distance", {}
                                    ).get("kilometers")
                                )
                                orbiting = approach.get(
                                    "orbiting_body", "Earth"
                                )

                                now = datetime.now(timezone.utc)
                                time_id = ensure_time_dimension(now)

                                cur.execute(
                                    """
                                    INSERT INTO fact_neo_approach
                                        (neo_reference_id, neo_name,
                                         time_id, absolute_magnitude,
                                         estimated_diameter_min,
                                         estimated_diameter_max,
                                         is_potentially_hazardous,
                                         close_approach_date,
                                         velocity_kph,
                                         miss_distance_km,
                                         orbiting_body, ingested_at)
                                    VALUES (%s, %s, %s, %s, %s, %s,
                                            %s, %s, %s, %s, %s, NOW())
                                    ON CONFLICT
                                        (neo_reference_id,
                                         close_approach_date)
                                    DO NOTHING
                                    """,
                                    (
                                        neo_id, neo_name, time_id,
                                        magnitude, diam_min, diam_max,
                                        is_hazardous, approach_date,
                                        velocity, miss_km, orbiting,
                                    ),
                                )
                                if cur.rowcount > 0:
                                    records_out += 1

        conn.commit()
    finally:
        conn.close()

    update_watermark(SOURCE_NAME, datetime.now(timezone.utc), len(new_files))

    log_audit(dag_id, "transform_nasa", run_id, "success",
              records_in=records_in, records_out=records_out)
    logger.info(f"NASA transform: {records_in} asteroids "
                f"-> {records_out} approaches loaded")


def _safe_float(val) -> float:
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None