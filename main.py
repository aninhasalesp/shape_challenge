from sqlalchemy import create_engine, inspect
from decouple import config
from models import Equipment, EquipmentSensor, EquipmentLog


DATABASE_URI = config("DATABASE_URI", default='sqlite:///database.sqlite')


def database_check(engine):
    for model in [Equipment, EquipmentSensor, EquipmentLog]:
        if not inspect(engine).has_table(model.__tablename__):
            model.__table__.create(engine)
            model.load_data(engine)


if __name__ == "__main__":
    engine = create_engine(DATABASE_URI, echo=True)
    database_check(engine)
