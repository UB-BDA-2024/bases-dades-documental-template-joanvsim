from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.mongodb_client import MongoDBClient
from app.redis_client import RedisClient
import json

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    add_collection_to_MD(mongodb_client=mongodb_client, sensor=sensor)
    return db_sensor

def add_collection_to_MD(mongodb_client: MongoDBClient, sensor: schemas.SensorCreate) :
    mycol = mongodb_client.getCollection(collection='sensor')
    sensor_db = {'name': sensor.name,
              'type': sensor.type, 
              'longitude': sensor.longitude,
              'latitude': sensor.latitude,
              'mac_address': sensor.mac_address,
              'manufacturer': sensor.manufacturer,
              'model': sensor.model,
              'serie_number': sensor.serie_number,
              'firmware_version': sensor.firmware_version}
    
    mycol.insert_one(sensor_db)

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData, sensor: models.Sensor, mongodb_client: MongoDBClient) -> schemas.Sensor:

    data_json = json.dumps({
        'velocity': data.velocity,
        'temperature': data.temperature,
        'humidity': data.humidity,
        'battery_level': data.battery_level,
        'last_seen': data.last_seen
    })

    redis.set(sensor_id, data_json)
    MD_sensor = get_sensor_collection_by_name(name=sensor.name, mongodb_client=mongodb_client)

    db_sensor = schemas.Sensor(
        id=sensor.id,
        name=sensor.name,
        latitude=MD_sensor.latitude,
        longitude=MD_sensor.longitude,
        joined_at=str(sensor.joined_at),
        last_seen=data.last_seen,
        type= MD_sensor.type,
        mac_address= MD_sensor.mac_address,
        battery_level = data.battery_level, 
        temperature=data.temperature, 
        humidity=data.humidity, 
        velocity=data.velocity
    )

    return db_sensor

def get_data(redis: Session, sensor_id: int, sensor: models.Sensor, mongodb_client: MongoDBClient) -> schemas.Sensor:

    data_json = redis.get(sensor_id)
    #Convertim el data_json en un diccionari manipulable per python
    data = json.loads(data_json)
    data = schemas.SensorData(
        velocity=data['velocity'],
        temperature=data['temperature'],
        humidity=data['humidity'],
        battery_level=data['battery_level'],
        last_seen=data['last_seen']
    )

    MD_sensor = get_sensor_collection_by_name(name=sensor.name,mongodb_client=mongodb_client)

    sensor_db = schemas.Sensor(
        id = sensor.id,
        name = sensor.name,
        latitude = MD_sensor.latitude,
        longitude = MD_sensor.longitude,
        joined_at = str(sensor.joined_at),
        last_seen = data.last_seen,
        type= MD_sensor.type,
        mac_address= MD_sensor.mac_address,
        battery_level = data.battery_level,
        temperature = data.temperature,
        humidity = data.humidity,
        velocity = data.velocity
    )

    return sensor_db

def get_sensor_collection_by_name(name: str, mongodb_client: MongoDBClient) -> schemas.SensorCreate:
    mycol = mongodb_client.getCollection(collection='sensor')
    MD_sensor = mycol.find_one({'name': name})
    
    return schemas.SensorCreate(
        name=name,
        type=MD_sensor['type'],
        longitude=MD_sensor['longitude'],
        latitude=MD_sensor['latitude'],
        mac_address=MD_sensor['mac_address'],
        manufacturer=MD_sensor['manufacturer'],
        model=MD_sensor['model'],
        serie_number=MD_sensor['serie_number'],
        firmware_version=MD_sensor['firmware_version']
    )

def delete_sensor(db: Session, sensor_id: int, redis: Session, mongodb_client: MongoDBClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    name = db_sensor.name

    #delete from PSQL
    db.delete(db_sensor)
    db.commit()

    #delete from MD
    mycol = mongodb_client.getCollection(collection='sensor')
    mycol.delete_one({'name': name})

    #delete from Redis
    redis.delete(sensor_id)
    return db_sensor

def get_sensors_near(mongodb_client: MongoDBClient, latitude: float, longitude: float, radius: float, redis: RedisClient, db: Session):
    near_sensors = []
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},
     "longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}

    mycol = mongodb_client.getCollection(collection='sensor')
    sensors = mycol.find(query)
    if sensors:
        for sensor in sensors:
            db_sensor = get_sensor_by_name(db, sensor['name'])
            db_sensor_data = get_data(redis, db_sensor.id)

            db_data = {
                'id': db_sensor.id,
                'name': db_sensor.name
            }

            db_data.update(db_sensor_data)
            near_sensors.append(db_data)
    else:
        print('There are no sensors.')

    return near_sensors