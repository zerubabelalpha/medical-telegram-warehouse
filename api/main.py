from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from typing import List
from . import models, schemas
from .database import engine, get_db

# Create tables in the database (marts schema should already exist from dbt)
# models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for querying analytical insights from the Telegram message data warehouse.",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"message": "Welcome to the Medical Telegram Analytics API", "docs": "/docs"}

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(limit: int = 10, db: Session = Depends(get_db)):
    """
    Returns the most frequently mentioned terms/products across all channels 
    based on YOLO detections.
    """
    results = db.query(
        models.ImageDetection.detected_class.label("product_name"),
        func.count(models.ImageDetection.detected_class).label("mention_count")
    ).group_by(
        models.ImageDetection.detected_class
    ).order_by(
        desc("mention_count")
    ).limit(limit).all()
    
    return results

@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(channel_name: str, db: Session = Depends(get_db)):
    """
    Returns posting activity and trends for a specific channel.
    """
    results = db.query(
        func.date(models.Message.message_datetime).label("date"),
        func.count(models.Message.message_key).label("message_count"),
        func.sum(models.Message.view_count).label("total_views"),
        func.sum(models.Message.forward_count).label("total_forwards")
    ).filter(
        models.Message.channel_name == channel_name
    ).group_by(
        func.date(models.Message.message_datetime)
    ).order_by(
        func.date(models.Message.message_datetime)
    ).all()
    
    if not results:
        raise HTTPException(status_code=404, detail="Channel not found or no activity recorded")
    
    return results

@app.get("/api/search/messages", response_model=List[schemas.MessageResponse])
def search_messages(query: str = Query(..., min_length=1), limit: int = 20, db: Session = Depends(get_db)):
    """
    Searches for messages containing a specific keyword.
    """
    results = db.query(models.Message).filter(
        models.Message.message_text.ilike(f"%{query}%")
    ).order_by(
        desc(models.Message.message_datetime)
    ).limit(limit).all()
    
    return results

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualContentStats])
def get_visual_content_stats(db: Session = Depends(get_db)):
    """
    Returns statistics about image usage and top object detections across channels.
    """
    # Get base message stats per channel
    channel_stats = db.query(
        models.Message.channel_name,
        func.count(models.Message.message_key).label("total_messages"),
        func.count(models.Message.message_key).filter(models.Message.has_image == True).label("messages_with_images")
    ).group_by(
        models.Message.channel_name
    ).all()
    
    final_stats = []
    for stat in channel_stats:
        # Get top 3 detected classes for this channel
        top_classes = db.query(
            models.ImageDetection.detected_class
        ).filter(
            models.ImageDetection.channel_key == db.query(models.Channel.channel_key).filter(models.Channel.channel_name == stat.channel_name).scalar_subquery()
        ).group_by(
            models.ImageDetection.detected_class
        ).order_by(
            desc(func.count(models.ImageDetection.detected_class))
        ).limit(3).all()
        
        final_stats.append({
            "channel_name": stat.channel_name,
            "total_messages": stat.total_messages,
            "messages_with_images": stat.messages_with_images,
            "image_percentage": (stat.messages_with_images / stat.total_messages * 100) if stat.total_messages > 0 else 0,
            "top_detected_classes": [c[0] for c in top_classes]
        })
        
    return final_stats
