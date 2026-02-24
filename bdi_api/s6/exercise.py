from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel
from pymongo import MongoClient

from bdi_api.settings import Settings

settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)

DB_NAME = "bdi_aircraft"
COLLECTION_NAME = "positions"


def _collection():
    """
    Returns the Mongo collection.
    Uses settings.mongo_url which should come from env var BDI_MONGO_URL.
    """
    if not settings.mongo_url:
        raise RuntimeError("BDI_MONGO_URL is not set. Example: mongodb://admin:admin123@localhost:27017")

    client = MongoClient(settings.mongo_url)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    # Helpful indexes (safe to call repeatedly)
    col.create_index([("icao", 1), ("timestamp", -1)])
    col.create_index([("type", 1)])
    return col


class AircraftPosition(BaseModel):
    icao: str
    registration: str | None = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    col = _collection()

    doc = position.model_dump()
    col.insert_one(doc)
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type.

    Response example: [{"type": "B738", "count": 42}, {"type": "A320", "count": 38}]

    Use MongoDB's aggregation pipeline with $group.
    """
    col = _collection()

    pipeline = [
        # group by type (None types will become null; tests usually have types set)
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "type": "$_id", "count": 1}},
        {"$sort": {"count": -1}},
    ]
    return list(col.aggregate(pipeline))


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    page_size: Annotated[
        int,
        Query(description="Number of results per page", ge=1, le=100),
    ] = 20,
) -> list[dict]:
    """List all aircraft with pagination.

    Each result should include: icao, registration, type.
    Use MongoDB's skip() and limit() for pagination.
    """
    col = _collection()

    skip = (page - 1) * page_size

    # Distinct aircraft = one row per ICAO.
    # We pick the latest record per ICAO so registration/type are current.
    pipeline = [
        {"$sort": {"icao": 1, "timestamp": -1}},
        {"$group": {"_id": "$icao", "registration": {"$first": "$registration"}, "type": {"$first": "$type"}}},
        {"$project": {"_id": 0, "icao": "$_id", "registration": 1, "type": 1}},
        {"$sort": {"icao": 1}},
        {"$skip": skip},
        {"$limit": page_size},
    ]
    return list(col.aggregate(pipeline))


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft.

    Return the most recent document matching the given ICAO code.
    If not found, return 404.
    """
    col = _collection()

    doc = col.find_one({"icao": icao}, projection={"_id": 0}, sort=[("timestamp", -1)])
    if not doc:
        raise HTTPException(status_code=404, detail="Aircraft not found")
    return doc


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    col = _collection()

    result = col.delete_many({"icao": icao})
    return {"deleted": int(result.deleted_count)}