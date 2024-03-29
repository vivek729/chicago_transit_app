"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
# places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# Define the input Kafka Topic.
topic = app.topic("cta_stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic("cta_stations_transformed", partitions=1)
# Define a Faust Table
table = app.Table(
   "cta_stations_transformed_table",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)

#
#
# Using Faust, transform input `Station` records into `TransformedStation` records.
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then set the `line` of the `TransformedStation` record to the string `"red"`
#
#

def transform_station(station):
    if station.red:
        line = "red"
    elif station.green:
        line = "green"
    else:
        line = "blue"
    return TransformedStation(station.station_id, station.station_name, station.order, line)
    
@app.agent(topic)
async def process_stations(stations_stream):
    async for station in stations_stream.filter(lambda x: x.red or x.green or x.blue):
        table[station.station_id] = transform_station(station)

if __name__ == "__main__":
    app.main()
