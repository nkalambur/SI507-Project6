# Import statements
import psycopg2
import psycopg2.extras
import csv
import re

####### Write code / functions to set up database connection and cursor here.


def get_connection_and_cursor(db_name, db_password, db_user):
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

####### Write code / functions to create tables with the columns you want and all database setup here.

def db_setup(db_name, db_password, db_user):
	conn, cur = get_connection_and_cursor(db_name, db_password, db_user)
	commands = (
		"""
		DROP TABLE IF EXISTS sites
		""",
		"""
		DROP TABLE IF EXISTS states
		""",
		"""
		CREATE TABLE states (
			ID SERIAL UNIQUE,
			Name VARCHAR(40) UNIQUE
			)""",
		"""
		CREATE TABLE sites (
			ID SERIAL, 
			Name VARCHAR(128) UNIQUE,
			Type VARCHAR(128),
			State_ID INT references states(ID),
			Location VARCHAR(255),
			Description TEXT
			) """ 
		)

	for command in commands:
		cur.execute(command)
		# print("Command Executed")
	conn.commit()

####### Write code / functions to deal with CSV files and insert data into the database here.

# Function to create and populate the states table - hardcoded to three states. 
def insert_into_states(db_name = "kalambur_507project6",db_password = "",db_user = "nikhilkalambur"):
	conn, cur = get_connection_and_cursor(db_name, db_password, db_user)

	cur.execute("INSERT INTO states (Name) VALUES ('Michigan')")
	cur.execute("INSERT INTO states (Name) VALUES ('Arkansas')")
	cur.execute("INSERT INTO states (Name) VALUES ('California')")

	conn.commit()

# Class to process data from csv into database ready data
	# Class to be used in insertion into sites table
class Convert_csvs(object):
	def __init__(self, state_dict):
		# Name of park
		self.name = state_dict["NAME"][0:127].strip().replace('\n','')
		# State Name
		self.state_name = re.findall(r'\/\s\w{2}\s\/', state_dict["ADDRESS"])[0].replace('/','').replace(' ','').strip().replace('\n','')
		# State ID - define this based on the file we're inserting..
		self.state_id = None
		# Type of Park
		self.type = state_dict["TYPE"][0:127].strip().replace('\n','')
		if state_dict["LOCATION"] == '':
			self.location = state_dict["ADDRESS"].strip().replace('\n','')
		else:
			self.location = state_dict["LOCATION"][0:254].strip().replace('\n','')
		self.desc = state_dict["DESCRIPTION"].strip().replace('\n','')

	def cols(self):
		return state_dict.keys()

	def upd_state_id(self, i):
		self.state_id = i

# Function to insert csv file into sql table
def insert_into_sites(fname):
	conn, cur = get_connection_and_cursor(db_name = "kalambur_507project6",db_password = "",db_user = "nikhilkalambur")
	with open(fname, 'r') as f:
		d = csv.DictReader(f)
		for row in d:
			c = Convert_csvs(row)
			# If statement to update the state_id, since some state records
			if "arkansas" in fname:
				c.upd_state_id(2)
			elif "michigan" in fname:
				c.upd_state_id(1)
			else:
				c.upd_state_id(3)
			cur.execute("""INSERT INTO sites (Name, Type, State_ID, Location, Description) VALUES(%s,%s,%s,%s,%s)""",
				(c.name,
				c.type,
				c.state_id,
				c.location,
				c.desc))
		conn.commit()

####### Make sure to commit your database changes with .commit() on the database connection.


####### Write code to be invoked here (e.g. invoking any functions you wrote above)
# Set up the db
db_setup(db_name = "kalambur_507project6", db_password = "", db_user = "nikhilkalambur")
# Populate the states table
insert_into_states()
# Insert into states
insert_into_sites("arkansas.csv")
insert_into_sites("michigan.csv")
insert_into_sites("california.csv")

####### Write code to make queries and save data in variables here.
print("########### QUERY 1 ALL LOCATIONS - SAVE TO all_locations ###########")
conn, cur = get_connection_and_cursor(db_name = "kalambur_507project6",db_password = "",db_user = "nikhilkalambur")
cur.execute("SELECT Location FROM sites")
all_locations = cur.fetchall()
print(all_locations)

print("########### QUERY 2 BEAUTIFUL SITES - SAVE TO beautiful_sites ###########")
cur.execute("SELECT name FROM sites WHERE description LIKE '%beautiful%'")
beautiful_sites = cur.fetchall()
print(beautiful_sites)

print("########### QUERY 3 NATL LAKESHORE - SAVE TO natl_lakeshores ###########")
cur.execute("SELECT count(*) FROM sites WHERE type LIKE '%National Lakeshore%'")
natl_lakeshores = cur.fetchall()
print(natl_lakeshores)

print("########### QUERY 4 MICH NAMES - SAVE TO michigan_names ###########")
cur.execute("SELECT sites.name FROM sites INNER JOIN states ON sites.state_id = states.id WHERE states.id = 1")
michigan_names = cur.fetchall()
print(michigan_names)

print("########### QUERY 5 TOTAL COUNT ARK - SAVE TO total_number_arkansas ###########")
cur.execute("SELECT count(*) FROM sites INNER JOIN states ON sites.state_id = states.id WHERE states.id = 2")
total_number_arkansas = cur.fetchall()
print(total_number_arkansas)

conn.close()
# We have not provided any tests, but you could write your own in this file or another file, if you want.
