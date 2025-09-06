from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Query
from arango import ArangoClient
from arango.exceptions import AQLQueryExecuteError

from app.core.config import settings

router = APIRouter()

def get_db():
    try:
        client = ArangoClient(hosts=settings.ARANGO_URL)
        db = client.db(settings.ARANGO_DB, username=settings.ARANGO_USER, password=settings.ARANGO_PASSWORD)
        if not db.has_collection(settings.ARANGO_COLLECTION):
            raise HTTPException(status_code=404, detail=f"Collection {settings.ARANGO_COLLECTION} not found")
        return db
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to ArangoDB: {e}")

@router.get("/data")
async def get_chart_data(
    symbol: str = Query("PIXELUSDT", description="The symbol to fetch data for."),
    start_timestamp: int = Query(default_factory=lambda: int((datetime.now() - timedelta(days=1)).timestamp() * 1000)),
    end_timestamp: int = Query(default_factory=lambda: int(datetime.now().timestamp() * 1000))
):
    db = get_db()
    collection = db.collection(settings.ARANGO_COLLECTION)
    aql = f"""
    FOR doc IN @@collection
        FILTER doc.symbol == @symbol
        FILTER doc.timestamp >= @start_ts AND doc.timestamp <= @end_ts
        SORT doc.timestamp ASC
        RETURN doc
    """
    bind_vars = {
        "@collection": settings.ARANGO_COLLECTION,
        "symbol": symbol,
        "start_ts": start_timestamp,
        "end_ts": end_timestamp
    }
    try:
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        return [doc for doc in cursor]
    except AQLQueryExecuteError as e:
        raise HTTPException(status_code=500, detail=f"AQL query failed: {e}")
