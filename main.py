from sqlalchemy import create_engine, inspect
from models import Equipment, EquipmentSensor, EquipmentLog


if __name__ == "__main__":
    engine = create_engine('sqlite:///database.sqlite', echo=True)

    for model in [Equipment, EquipmentSensor, EquipmentLog]:
        if not inspect(engine).has_table(model.__tablename__):
            model.__table__.create(engine)
            model.load_data(engine)
