from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData, sensor: models.Sensor) -> schemas.Sensor:
    sensor_key_ls = f"{sensor_id}:last_seen"
    sensor_key_temp = f"{sensor_id}:temperature"
    sensor_key_humi = f"{sensor_id}:humidity"
    sensor_key_bl = f"{sensor_id}:battery_level"

    db_sensor = schemas.Sensor(
        id=sensor_id,
        name=sensor.name,
        latitude=sensor.latitude,
        longitude=sensor.longitude,
        joined_at=str(sensor.joined_at),
        last_seen=data.last_seen, 
        temperature=data.temperature, 
        humidity=data.humidity, 
        battery_level=data.battery_level
    )
    
    redis.set(sensor_key_ls, data.last_seen)
    redis.set(sensor_key_temp, data.temperature)
    redis.set(sensor_key_humi, data.humidity)
    redis.set(sensor_key_bl, data.battery_level)
    
    return db_sensor

def get_data(redis: Session, sensor_id: int, sensor: models.Sensor) -> schemas.Sensor:
    sensor_ls = redis.get(f"{sensor_id}:last_seen")
    sensor_temp = redis.get(f"{sensor_id}:temperature")
    sensor_humi = redis.get(f"{sensor_id}:humidity")
    sensor_bl = redis.get(f"{sensor_id}:battery_level")

    db_sensor = schemas.Sensor(
        id=sensor_id,
        name=sensor.name,
        latitude=sensor.latitude,
        longitude=sensor.longitude,
        joined_at=str(sensor.joined_at),
        last_seen=sensor_ls, 
        temperature=sensor_temp, 
        humidity=sensor_humi, 
        battery_level=sensor_bl
    )
    
    return db_sensor

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor