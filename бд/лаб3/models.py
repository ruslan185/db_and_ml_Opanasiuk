from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
from datetime import datetime

Base = declarative_base()

class Weather(Base):
    __tablename__ = 'weather'

    id = Column(Integer, primary_key=True)
    country = Column(String)
    wind_degree = Column(Integer)
    wind_kph = Column(Float)
    wind_direction = Column(String)
    last_updated = Column(Date)
    sunrise = Column(Time)
    
    precip_mm = Column(Float)
    precip_in = Column(Float)
    humidity = Column(Integer)
    cloud = Column(Integer)

def populate_db(session, csv_file):
    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            weather = Weather(
                country=row['country'],
                wind_degree=int(row['wind_degree']),
                wind_kph=float(row['wind_kph']),
                wind_direction=row['wind_direction'],
                last_updated=datetime.strptime(row['last_updated'], '%Y-%m-%d %H:%M').date(),
                sunrise=datetime.strptime(row['sunrise'], '%I:%M %p').time(),
                
                precip_mm=float(row['precip_mm']),
                precip_in=float(row['precip_in']),
                humidity=int(row['humidity']),
                cloud=int(row['cloud'])
            )
            session.add(weather)
        session.commit()

def create_database(connection_string):
    engine = create_engine(connection_string)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


if __name__ == "__main__":
    username = 'ruslan'
    password = '1111'
    database = 'lab3DB'
    host = 'localhost'
    port = '5432'
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'
    session = create_database(connection_string)
    populate_db(session, 'GlobalWeatherRepository.csv')
