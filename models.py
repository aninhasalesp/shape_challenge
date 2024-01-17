import re
import json
import csv
import logging

from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, DateTime
from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.exc import IntegrityError

Base = declarative_base()


class Equipment(Base):
    __tablename__ = "equipment"

    equipment_id = Column(Integer, unique=True, primary_key=True)
    name = Column(String)
    group_name = Column(String)
    sensors = relationship("EquipmentSensor", back_populates="equipment")

    @classmethod
    def load_data(cls, engine):
        return cls.load_from_json(f'resource/{cls.__tablename__}.json', engine)

    @staticmethod
    def bulk_create(equipment_list, engine):
        session = Session(engine)
        session.bulk_save_objects(equipment_list)
        session.commit()

    @classmethod
    def load_from_json(cls, path_json, engine):
        with open(path_json, "r") as f:
            equipment_list = json.load(f)
        cls.bulk_create(
            [cls(**equipment) for equipment in equipment_list],
            engine
        )
        return equipment_list


class EquipmentSensor(Base):
    __tablename__ = "equipment_sensors"
    __table_args__ = (
        UniqueConstraint("sensor_id", "equipment_id"),
    )

    sensor_id = Column(Integer, unique=True, primary_key=True)
    equipment_id = Column(Integer, ForeignKey("equipment.equipment_id"))
    equipment = relationship("Equipment", back_populates="sensors")

    logs = relationship("EquipmentLog", back_populates="sensor")

    @classmethod
    def load_data(cls, engine):
        return cls.load_from_csv(f'resource/{cls.__tablename__}.csv', engine)

    @staticmethod
    def bulk_create(reader, engine):
        session = Session(engine)
        session.bulk_save_objects(reader)
        session.commit()

    @classmethod
    def load_from_csv(cls, path_csv, engine):
        with open(path_csv, newline='') as arquivo_csv:
            sensors_list = list(csv.DictReader(arquivo_csv))

        cls.bulk_create(
            [cls(**sensor) for sensor in sensors_list],
            engine
        )
        return sensors_list


class EquipmentLog(Base):
    __tablename__ = "equipment_failure_sensors"

    id = Column(Integer, primary_key=True, unique=True, autoincrement=True)
    timestamp = Column(DateTime)
    status = Column(String)
    temperature = Column(Float)
    vibration = Column(Float)

    sensor_id = Column(Integer, ForeignKey("equipment_sensors.sensor_id"))
    sensor = relationship("EquipmentSensor", back_populates="logs")

    @classmethod
    def load_data(cls, engine):
        return cls.load_from_txt(
            f'resource/{cls.__tablename__}.txt', engine, bulk=True
        )
        return

    @staticmethod
    def bulk_create(equipment_logs, engine):
        with Session(engine) as session:
            session.bulk_save_objects(equipment_logs)
            session.commit()
        return equipment_logs

    @staticmethod
    def create(equipment_log, engine):
        with Session(engine) as session:
            session.add(equipment_log)
            session.commit()
        return equipment_log

    @classmethod
    def load_from_txt(cls, path_txt, engine, bulk=True):
        if bulk:
            with open(path_txt, 'r') as arquivo:
                return cls.bulk_create(
                    [cls(**cls.parse_log_line(linha)) for linha in arquivo.readlines()],
                    engine
                )

        equipment_logs = []
        with open(path_txt, 'r') as log_file:
            for log_line in log_file:
                try:
                    equipment_logs.append(
                        cls.create(cls(**cls.parse_log_line(log_line)), engine)
                    )
                except IntegrityError as error:
                    logging.error(error.statement)
        return equipment_logs

    @staticmethod
    def parse_log_line(linha: str) -> dict:
        match = re.match(
            (
                r"\[(.*?)\]\s+([A-Z]+)\s+sensor\[(\d+)\]:"
                r"\s+\(temperature\s+(-?[\d.]+|\berr\b),"
                r"\s+vibration\s+(-?[\d.]+|\berr\b)\)"
            ),
            linha
        )
        
        if match:
            timestamp, status, sensor_id, temperature, vibration = match.groups()

            try:
                parsed_timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                parsed_timestamp = datetime.strptime(timestamp, "%Y/%m/%d")

            return {
                'timestamp': parsed_timestamp,
                'status': status,
                'sensor_id': int(sensor_id),
                'temperature': None if temperature == 'err' else float(temperature),
                'vibration': None if vibration == 'err' else float(vibration)
            }
        raise Exception("Log parse error")
