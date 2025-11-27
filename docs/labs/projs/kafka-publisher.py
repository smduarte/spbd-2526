"""
Created on Mon Apr 29 00:51:20 2019

@author: nmp
"""

import time
import sys
import datetime
import threading
import argparse
import dataclasses, json
from dataclasses import dataclass
from typing import Optional

@dataclass
class TaxiTrip:
    medallion: str                       # md5 sum of taxi identifier
    hack_license: str                    # md5 sum of license identifier
    pickup_datetime: datetime            # pickup timestamp
    dropoff_datetime: datetime           # dropoff timestamp
    trip_time_in_secs: int               # duration of trip
    trip_distance: float                 # distance in miles
    pickup_longitude: float              # pickup longitude
    pickup_latitude: float               # pickup latitude
    dropoff_longitude: float             # dropoff longitude
    dropoff_latitude: float              # dropoff latitude
    payment_type: str                    # payment method (credit/cash)
    fare_amount: float                   # fare in dollars
    surcharge: float                     # surcharge in dollars
    mta_tax: float                       # MTA tax in dollars
    tip_amount: float                    # tip in dollars
    tolls_amount: float                  # tolls in dollars
    total_amount: float                  # total paid in dollars	

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

def parse_taxi_trip_line(line: str) -> Optional[TaxiTrip]:
    """
    Parse a CSV line into a TaxiTrip instance.
    Returns None if the line is malformed.
    """

    parts = [p.strip() for p in line.split(",")]

    # Check field count
    if len(parts) != 17:
        # print(f"Malformed line (wrong number of fields): {line}")
        return None

    try:
        pickup_dt = datetime.datetime.strptime(parts[2], TIMESTAMP_FORMAT)
        dropoff_dt = datetime.datetime.strptime(parts[3], TIMESTAMP_FORMAT)

        return TaxiTrip(
            medallion=parts[0],
            hack_license=parts[1],
            pickup_datetime=pickup_dt,
            dropoff_datetime=dropoff_dt,
            trip_time_in_secs=int(parts[4]),
            trip_distance=float(parts[5]),
            pickup_longitude=float(parts[6]),
            pickup_latitude=float(parts[7]),
            dropoff_longitude=float(parts[8]),
            dropoff_latitude=float(parts[9]),
            payment_type=parts[10],
            fare_amount=float(parts[11]),
            surcharge=float(parts[12]),
            mta_tax=float(parts[13]),
            tip_amount=float(parts[14]),
            tolls_amount=float(parts[15]),
            total_amount=float(parts[16]),
        )

    except Exception as e:
        print(e)
        # print(f"Malformed line (conversion error): {line}")
        return None  	

import json
from dataclasses import asdict

def taxi_trip_to_json(trip: TaxiTrip) -> str:
    """
    Serialize a TaxiTrip dataclass to a JSON string.
    Datetimes are converted to ISO-8601 strings.
    """
    data = asdict(trip)
    data["pickup_datetime"] = trip.pickup_datetime.isoformat(sep=" ")
    data["dropoff_datetime"] = trip.dropoff_datetime.isoformat(sep=" ")
    return json.dumps(data)

from kafka import KafkaProducer

def publish(taxi_rides, topic, speedup) :
    try: 
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))
        
        firstRideTime = next(taxi_rides).dropoff_datetime
        firstWallTime = datetime.datetime.now()

        for ride in taxi_rides:
            try:
                rideTime = ride.dropoff_datetime
                deltaLineTime = rideTime - firstRideTime
                
                deltaLineTimeS = (rideTime - firstRideTime) / datetime.timedelta(microseconds=1) / 1000000.0
                
                #print('ride relative time: : {} secs'.format(deltaLineTimeS))

                wallTime = datetime.datetime.now()
                deltaWallTimeS = (wallTime - firstWallTime) / datetime.timedelta(microseconds=1) / 1000000.0
                
                #print('wall relative time: : {} secs'.format(deltaWallTimeS))
                
                delay = (deltaLineTimeS/speedup - deltaWallTimeS)

                #print( 'wall time: {}, delay: {}, {}'.format(deltaWallTimeS, delay, 1.0/delay))
		
                if delay > 0 :
                    time.sleep( delay )
                    
                ride_duration = ride.dropoff_datetime - ride.pickup_datetime
                
                ride.dropoff_datetime = datetime.datetime.now()

                ride.pickup_datetime = ride.dropoff_datetime - ride_duration
                                    
                producer.send(topic, value=taxi_trip_to_json(ride) )

                # print( taxi_trip_to_json(ride) )

            except Exception as err:
                print(err)
                
    except Exception as err:
            print(err)



import gzip

def read_taxi_trips_from_gz(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        next(f) # skip head
        for line in f:
            line = line.strip()
            if not line:
                continue

            trip = parse_taxi_trip_line(line)
            if trip is not None:
                yield trip


parser = argparse.ArgumentParser(description='dataset kafka publisher...')
parser.add_argument('--filename', type=str, default='taxi_rides_1pc.csv.gz', help='gzipped dataset filename .gz') 
parser.add_argument('--topic', dest='topic', type=str, default='taxis_json', help='kafka topic (default: taxis_json)')
parser.add_argument('--speedup', type=int, dest='speedup', default=60, help='time speedup factor (default: 60)')

args = parser.parse_args()
print(args)

publish(read_taxi_trips_from_gz(args.filename), args.topic, args.speedup)
