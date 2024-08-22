import logging
from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, BackgroundTasks, Request, Body
from sqlalchemy.orm import Session
from db import SessionLocal
from crud import create_event

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()


@app.post("/orders")
async def process_event(body: Annotated[bytes, Body(media_type="application/xml")], db: Session = Depends(get_db)):
    try:
        create_event(db, body)
        return {"result": "success"}
    except:
        logging.exception("failed")
        raise HTTPException(status_code=500, detail="Failed to insert an event")
