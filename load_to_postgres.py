import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, DateTime, Float

def get_dataframe():
    file_csv = "./dataset/covid_data.csv"
    df = pd.read_csv(file_csv)
    return df

def get_postgres_conn():
	user = 'postgres'
	password = 'pass'
	host = 'localhost'
	database = 'covid19'
	port = 5432

	conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
	engine = create_engine(conn_string)
	return engine

def load_to_postgres(engine):
	df_schema = {
	'date' : DateTime,
	'location_iso_code': String,
	'location': String,
	'new_cases': BigInteger,
	'new_deaths': BigInteger,
	'new_recovered': BigInteger,
	'new_active_cases': BigInteger,
	'total_cases': BigInteger,
	'total_deaths': BigInteger,
	'total_recovered': BigInteger,
	'total_active_cases': BigInteger,
	'location_level': String,
	'province': String,
	'country': String,
	'continent': String,
	'island': String,
	'time_zone': String,
	'total_cities': Float,
	'total_regencies': Float,
	'total_districts': BigInteger,
	'total_urban_villages': Float,
	'total_rural_villages': Float,
	'area_km2': BigInteger,
	'population': BigInteger,
	'population_density': Float,
	'longitude': Float,
	'latitude': Float,
	'new_cases_per_million': Float,
	'total_cases_per_million': Float,
	'new_deaths_per_million': Float,
	'total_deaths_per_million': Float,
	'total_deaths_per_100rb': Float,
	'case_fatality_rate': Float,
	'case_recovered_rate': Float,
	'growth_factor_of_new_cases': Float,
	'growth_factor_of_new_deaths': Float
	}
	df.to_sql(name='covid_indonesia', con=engine, if_exists='replace', index=False, schema='public', dtype=df_schema, method=None, chunksize=5000)
      

df = get_dataframe()
postgres_conn = get_postgres_conn()
load_to_postgres(postgres_conn)