from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date, Time, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class WeatherMain(Base):
    __tablename__ = 'weather_main'
    id = Column(Integer, primary_key=True)
    country = Column(String)
    wind_degree = Column(Integer)
    wind_direction = Column(String)
    last_updated = Column(Date)
    sunrise = Column(Time)
    precipitations = relationship('Precipitation', back_populates='weather_main')

class Precipitation(Base):
    __tablename__ = 'precipitation'
    id = Column(Integer, primary_key=True)
    weather_main_id = Column(Integer, ForeignKey('weather_main.id'))
    precip_mm = Column(Float)
    wind_kph = Column(Float)
    precip_in = Column(Float)
    humidity = Column(Integer)
    cloud = Column(Integer)
    go_outside = Column(sa.Boolean)
    weather_main = relationship('WeatherMain', back_populates='precipitations')

def query_weather(session, country, date):
    date = datetime.strptime(date, '%Y-%m-%d').date()

    results = session.query(WeatherMain, Precipitation).join(Precipitation, WeatherMain.id == Precipitation.weather_main_id).filter(
        WeatherMain.country == country,
        WeatherMain.last_updated == date
    ).all()

    if results:
        for weather_main, precipitation in results:
            print(f"Country: {weather_main.country}")
            print(f"Wind Degree: {weather_main.wind_degree}")
            print(f"Wind Direction: {weather_main.wind_direction}")
            print(f"Last Updated: {weather_main.last_updated}")
            print(f"Sunrise: {weather_main.sunrise}")
            print(f"Precipitation mm: {precipitation.precip_mm}")
            print(f"Precipitation in: {precipitation.precip_in}")
            print(f"Humidity: {precipitation.humidity}")
            print(f"Cloud: {precipitation.cloud}")
            print(f"Wind KPH: {precipitation.wind_kph}")
            print(f"Go Outside: {precipitation.go_outside}")
            print("")
    else:
        print(f"No weather data found for country: {country} on date: {date}")

if __name__ == "__main__":

    username = 'ruslan'
    password = '1111'
    database = 'lab3DB'
    host = 'localhost'
    port = '5432'
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    country = input("Enter the country: ")
    date = input("Enter the date (YYYY-MM-DD): ")

    query_weather(session, country, date)

