import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
from datetime import datetime, timedelta
import csv
from io import StringIO

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI Backend!"}

@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}

@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }
    
    try:
        # Try to import database module
        from database import db
        
        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"
            
            # Try to list collections to verify connectivity
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]  # Show first 10 collections
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"
            
    except ImportError:
        response["database"] = "❌ Database module not found (run enable-database first)"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"
    
    # Check environment variables
    import os
    response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"
    
    return response

# ---------------------- Optimization Endpoint ----------------------

def parse_demand_csv(content: str, city: str) -> Dict[datetime, int]:
    reader = csv.DictReader(StringIO(content))
    demand_map: Dict[datetime, int] = {}
    for row in reader:
        try:
            if row.get('city', '').strip() != city:
                continue
            ts = row.get('timestamp') or row.get('time') or row.get('datetime')
            if not ts:
                continue
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00').replace(' ', 'T'))
            demand = int(float(row.get('demand', '0')))
            demand_map[dt] = demand
        except Exception:
            # skip malformed rows
            continue
    return demand_map


def parse_riders_csv(content: str, city: str) -> List[Dict[str, Any]]:
    """
    Expected columns: rider_id, city, start, end (ISO or 'YYYY-MM-DD HH:MM')
    Multiple rows per rider allowed (multiple shifts)
    """
    reader = csv.DictReader(StringIO(content))
    riders: List[Dict[str, Any]] = []
    for row in reader:
        try:
            if row.get('city', '').strip() != city:
                continue
            start_raw = row.get('start') or row.get('start_time')
            end_raw = row.get('end') or row.get('end_time')
            if not start_raw or not end_raw:
                continue
            start_dt = datetime.fromisoformat(start_raw.replace('Z', '+00:00').replace(' ', 'T'))
            end_dt = datetime.fromisoformat(end_raw.replace('Z', '+00:00').replace(' ', 'T'))
            if end_dt <= start_dt:
                continue
            riders.append({
                'rider_id': row.get('rider_id') or row.get('id') or '',
                'start': start_dt,
                'end': end_dt
            })
        except Exception:
            continue
    return riders


def daterange(start: datetime, end: datetime, step_minutes: int = 30) -> List[datetime]:
    out = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = cur + timedelta(minutes=step_minutes)
    return out


def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


@app.post("/api/optimize")
async def optimize(
    demand_file: UploadFile = File(...),
    riders_file: UploadFile = File(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    city: str = Form(...),
    interval_minutes: int = Form(30)
):
    """
    Accepts two CSV files and parameters, returns an optimization summary and time series.
    - demand_file CSV: columns [timestamp, demand, city]
    - riders_file CSV: columns [rider_id, city, start, end]
    - start_date/end_date: 'YYYY-MM-DD HH:MM' or ISO format
    - city: filter code (e.g., MAD)
    """
    try:
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00').replace(' ', 'T'))
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00').replace(' ', 'T'))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format. Use 'YYYY-MM-DD HH:MM'.")

    if end_dt <= start_dt:
        raise HTTPException(status_code=400, detail="end_date must be after start_date")

    demand_text = (await demand_file.read()).decode('utf-8', errors='ignore')
    riders_text = (await riders_file.read()).decode('utf-8', errors='ignore')

    demand_map = parse_demand_csv(demand_text, city.strip())
    riders = parse_riders_csv(riders_text, city.strip())

    # Build timeline
    times = daterange(start_dt, end_dt, interval_minutes)

    series: List[Dict[str, Any]] = []
    total_unmet = 0
    total_surplus = 0

    for i in range(len(times)):
        t0 = times[i]
        t1 = times[i] + timedelta(minutes=interval_minutes)
        demand = demand_map.get(t0, 0)
        available = sum(1 for r in riders if overlap(r['start'], r['end'], t0, t1))
        staffed = min(demand, available)
        unmet = max(demand - available, 0)
        surplus = max(available - demand, 0)
        total_unmet += unmet
        total_surplus += surplus
        series.append({
            'time': t0.isoformat(),
            'demand': demand,
            'available': available,
            'staffed': staffed,
            'unmet': unmet,
            'surplus': surplus
        })

    summary = {
        'interval_minutes': interval_minutes,
        'points': len(series),
        'total_unmet': total_unmet,
        'total_surplus': total_surplus,
        'city': city,
        'start': start_dt.isoformat(),
        'end': end_dt.isoformat(),
    }

    return {"summary": summary, "series": series}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
