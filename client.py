from msilib.schema import tables
from pyModbusTCP.client import ModbusClient
import logging
import psycopg2
import datetime
from db_config import dbConfig
import  time



# TCP auto connect on modbus request, close after it

client = ModbusClient(host="127.0.0.1", port=5020, unit_id=1, auto_open=True, auto_close=True) 

# Query for save the data into postgresql database.
postgresql_query = """INSERT INTO valores (ciudad, hora, clima) VALUES (%s,%s,%s);"""

FORMAT = ('%(asctime)-15s %(threadName)-15s'
          ' %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s')
logging.basicConfig(format=FORMAT)
log = logging.getLogger()
log.setLevel(logging.DEBUG)

try:
    params = dbConfig()
    connection = None;
    print("Starting connection to db")
    connection = psycopg2.connect(**params)
    print("Connection to db succesfully")

    x=True

    while x==True:
        
        value = client.read_holding_registers(0, 2)
        temp_buc=value[0]/100
        temp_pue=value[1]/100

        dt = datetime.datetime.now()
    

        
        print(f"Bucaramanga: {temp_buc}") 
        print(f"Puebla: {temp_pue}")
        print(f"dt: {dt}")

        cursor = connection.cursor()
        cursor.execute(postgresql_query, ("Bucaramanga",  dt, temp_buc))
        cursor.execute(postgresql_query, ("Puebla",  dt , temp_pue))
        connection.commit()
        cursor.close()

        
        time.sleep(30)
        print(value)
except:
    print("Client is offline")
finally:
    if connection is not None:
        connection.close()
        print('Connection with database closed.')

        







