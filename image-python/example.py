#!/usr/bin/env python
import csv
import json
import time
import sqlalchemy
from sqlalchemy.exc import OperationalError

# Connect to the MySQL database with retry mechanism
engine = sqlalchemy.create_engine("mysql://user_dteng:cirilgroupt@database/db_dteng")
for i in range(10):
    try:
        connection = engine.connect()
        print("Connected to MySQL!")
        break
    except OperationalError as e:
        print(f"MySQL not ready yet (attempt {i+1}/10)...")
        time.sleep(5)
else:
    raise Exception("Could not connect to MySQL after 10 attempts")


metadata = sqlalchemy.schema.MetaData(engine)

# Define ORM objects for the database tables
# These objects will be used to interact with the database tables: examples, places, and people
Example = sqlalchemy.schema.Table('examples', metadata, autoload=True, autoload_with=engine)
Place = sqlalchemy.schema.Table('places', metadata, autoload=True, autoload_with=engine)
Person = sqlalchemy.schema.Table('people', metadata, autoload=True, autoload_with=engine)

# Read and import data from example.csv into the examples table
with open('/data/example.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for row in reader: 
    connection.execute(Example.insert().values(name = row[0]))

# Read and import data from places.csv into the places table
with open('/data/places.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)  # Skip the header row
  for row in reader: 
    connection.execute(Place.insert().values(city = row[0], county = row[1], country = row[2]))

# Read and import data from people.csv into the people table
# This requires looking up place_of_birth_id from the places table
with open('/data/people.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)  # Skip the header row
  for row in reader: 
    place_of_birth_id = connection.execute(sqlalchemy.sql.select([Place.c.id]).where(
        (Place.c.city == row[3])
    )).fetchone()
    connection.execute(Person.insert().values(given_name = row[0], family_name = row[1], date_of_birth = row[2], place_of_birth_id = place_of_birth_id[0]))

# Export examples table data to JSON format
with open('/data/example_python.json', 'w') as json_file:
  # Query all rows from the Example table
  rows = connection.execute(sqlalchemy.sql.select([Example])).fetchall()
  # Convert rows to list of dictionaries
  rows = [{'id': row[0], 'name': row[1]} for row in rows]
  # Write to JSON file with minimal whitespace
  json.dump(rows, json_file, separators=(',', ':'))

# output the table people to a JSON file
with open('/data/example_person.json', 'w') as json_file:
  rows = connection.execute(sqlalchemy.sql.select([Person])).fetchall()
  rows = [{'id': row[0], 'given_name': row[1], 'family_name': row[2], 'date_of_birth': row[3], 'place_of_birth_id': row[4]} for row in rows]
  json.dump(rows, json_file, separators=(',', ':'))

# output the table places to a JSON file
with open('/data/example_place.json', 'w') as json_file:
  rows = connection.execute(sqlalchemy.sql.select([Place])).fetchall()
  rows = [{'id': row[0], 'city': row[1], 'county': row[2], 'country': row[3]} for row in rows]
  json.dump(rows, json_file, separators=(',', ':'))

# output the table to a JSON file
with open('/data/example_place.json', 'w') as json_file:
  rows = connection.execute(sqlalchemy.sql.select([Place])).fetchall()
  rows = [{'id': row[0], 'city': row[1], 'county': row[2], 'country': row[3]} for row in rows]
  json.dump(rows, json_file, separators=(',', ':'))

# Generate population summary by country and save to output.json
with open('/data/output.json', 'w') as json_file:
  # Create a query to count the number of people born in each country
  # This joins the people and places tables and groups the results by country
  query = sqlalchemy.sql.select([
      Place.c.country,
      sqlalchemy.sql.func.count(Person.c.id).label('population')
  ]).select_from(
      Person.join(Place, Person.c.place_of_birth_id == Place.c.id)
  ).group_by(Place.c.country)

  rows = connection.execute(query).fetchall()

  # Create a dictionary with country as key and population count as value
  result = {row[0]: row[1] for row in rows}

  # Write to JSON file
  json.dump(result, json_file, separators=(',', ':'))