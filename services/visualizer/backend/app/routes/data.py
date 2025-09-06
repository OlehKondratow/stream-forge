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
    # Generate 100 hardcoded data points for testing
    generated_data = []
    base_timestamp = 1678886400000 # March 15, 2023, 00:00:00 UTC
    for i in range(100):
        timestamp = base_timestamp + (i * 60 * 1000) # 1 minute intervals
        open_price = 100 + (i * 0.5)
        close_price = open_price + (2 if i % 2 == 0 else -2) # Simple up/down pattern
        high_price = max(open_price, close_price) + 1
        low_price = min(open_price, close_price) - 1
        volume = 1000 + (i * 10)

        generated_data.append({
            "timestamp": timestamp,
            "candle": {
                "open": round(open_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "close": round(close_price, 2),
                "volume": round(volume, 2)
            },
            "indicators": {
                "rsi_14": 50 + (i % 5),
                "MACD_12_26_9": 1 + (i * 0.01),
                "MACDs_12_26_9": 0.5 + (i * 0.005),
                "MACDh_12_26_9": 0.5 + (i * 0.005)
            },
            "volume_profile": {
                "100": 500 + (i * 5),
                "105": 500 + (i * 5)
            }
        })
    return generated_data
    # db = get_db()
    # collection = db.collection(settings.ARANGO_COLLECTION)
    # aql = f"""
    # FOR doc IN @@collection
    #     FILTER doc.symbol == @symbol
    #     FILTER doc.timestamp >= @start_ts AND doc.timestamp <= @end_ts
    #     SORT doc.timestamp ASC
    #     RETURN doc
    # """
    # bind_vars = {
    #     "@collection": settings.ARANGO_COLLECTION,
    #     "symbol": symbol,
    #     "start_ts": start_timestamp,
    #     "end_ts": end_timestamp
    # }
    # try:
    #     cursor = db.aql.execute(aql, bind_vars=bind_vars)
    #     return [doc for doc in cursor]
    # except AQLQueryExecuteError as e:
    #     raise HTTPException(status_code=500, detail=f"AQL query failed: {e}")
