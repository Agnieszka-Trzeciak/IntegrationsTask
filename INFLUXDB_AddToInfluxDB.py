from datetime import datetime
from influxdb import InfluxDBClient
import influxdb_client_3
from INFLUXDB_GetData import Fetch_Data
import os

with open('Bearer_Token.txt','r') as token_file:
        token = token_file.readline()
with open('Client_Token.txt','r') as token_file:
        c_token = token_file.readline()


client = InfluxDBClient(    host='localhost',
                            port=8181,
                            headers={'Authorization': token})
client.switch_database('WeatherDB')

END_DATE = datetime.today().strftime('%Y-%m-%d')

query = 'SELECT * FROM "weather_data_final" ORDER BY "time" DESC LIMIT 1'
client_version3 = influxdb_client_3.InfluxDBClient3(host='http://localhost:8181',org='trzeciakagnieszka98@gmail.com',database="WeatherDB", token=c_token)
results = client_version3.query(query=query,language="influxql")
START_DATE = str(results.to_pandas()['time'].values[0].astype('datetime64[D]'))

Data = Fetch_Data(START_DATE,END_DATE)
client.write_points(Data,batch_size=10000, protocol='line',time_precision='n')
print(f'Added results from {START_DATE} to {END_DATE}.')

