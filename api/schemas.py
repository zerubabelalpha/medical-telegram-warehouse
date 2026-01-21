from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, date

class TopProduct(BaseModel):
    product_name: str
    mention_count: int

    class Config:
        orm_mode = True

class ChannelActivity(BaseModel):
    date: date
    message_count: int
    total_views: int
    total_forwards: int

    class Config:
        orm_mode = True

class MessageResponse(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    message_datetime: datetime
    view_count: int
    forward_count: int
    has_image: bool

    class Config:
        orm_mode = True

class VisualContentStats(BaseModel):
    channel_name: str
    total_messages: int
    messages_with_images: int
    image_percentage: float
    top_detected_classes: List[str]

    class Config:
        orm_mode = True
