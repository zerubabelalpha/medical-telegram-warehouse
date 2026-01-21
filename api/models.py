from sqlalchemy import Column, Integer, String, Boolean, Float, DateTime, Date, ForeignKey, Text
from sqlalchemy.orm import relationship
from .database import Base

class Channel(Base):
    __tablename__ = "dim_channels"
    __table_args__ = {"schema": "marts"}

    channel_key = Column(String, primary_key=True, index=True)
    channel_name = Column(String, unique=True, index=True, nullable=False)
    channel_type = Column(String)
    first_post_date = Column(Date)
    last_post_date = Column(Date)
    total_posts = Column(Integer)
    avg_views = Column(Float)

    messages = relationship("Message", back_populates="channel")

class DateDim(Base):
    __tablename__ = "dim_dates"
    __table_args__ = {"schema": "marts"}

    date_key = Column(Integer, primary_key=True, index=True)
    full_date = Column(Date, unique=True, index=True, nullable=False)
    day_of_month = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(String)
    week_of_year = Column(Integer)
    month = Column(Integer)
    month_name = Column(String)
    quarter = Column(Integer)
    year = Column(Integer)
    is_weekend = Column(Boolean)

class Message(Base):
    __tablename__ = "fct_messages"
    __table_args__ = {"schema": "marts"}

    message_key = Column(String, primary_key=True, index=True)
    message_id = Column(Integer)
    channel_name = Column(String)
    channel_key = Column(String, ForeignKey("marts.dim_channels.channel_key"))
    date_key = Column(Integer, ForeignKey("marts.dim_dates.date_key"))
    message_text = Column(Text)
    message_length = Column(Integer)
    view_count = Column(Integer)
    forward_count = Column(Integer)
    has_image = Column(Boolean)
    message_datetime = Column(DateTime)

    channel = relationship("Channel", back_populates="messages")
    detections = relationship("ImageDetection", back_populates="message")

class ImageDetection(Base):
    __tablename__ = "fct_image_detections"
    __table_args__ = {"schema": "marts"}

    # fct_image_detections doesn't have a specific primary key in the schema.yml
    # but SQLAlchemy needs one. I'll use message_key and detected_class as composite key if needed,
    # or just message_key if it's 1:1, but usually it's 1:N.
    # Actually, looking at the dbt model fct_image_detections.sql would help.
    
    # For now, let's treat message_key + detected_class as a primary key or just add an ID if possible.
    # If dbt doesn't have a PK, I might have to use a composite one.
    message_key = Column(String, ForeignKey("marts.fct_messages.message_key"), primary_key=True)
    detected_class = Column(String, primary_key=True)
    message_id = Column(Integer)
    channel_key = Column(String)
    date_key = Column(Integer)
    confidence_score = Column(Float)
    image_category = Column(String)

    message = relationship("Message", back_populates="detections")
