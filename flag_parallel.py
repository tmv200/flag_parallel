####################################################################################################
#                                                                                                  # 
#                     Parallel implementation using Apache Spark                                   #
#                                                                                                  #
####################################################################################################

from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext("local[*]", "ais")

import sys
import ais
import os
import re
import time
import math
import copy
from time import sleep
from math import sin, cos, sqrt, atan2, radians
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import first


####################################################################################################

def distance(y1,x1,y2,x2):

    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(y1)
    lon1 = radians(x1)
    lat2 = radians(y2)
    lon2 = radians(x2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c * 1000
    return distance

def check_distances(partition, region1, y1, x1, message1, result):
    region_reached = False
    for message2 in partition:
        region2 = message2[4]
        if region1 == region2:
            region_reached = True
            ships_distance = distance(y1, x1, message2[1], message2[2])
            if (0.0 < float(ships_distance) < 100):
                flagged = [message1[0], message1[1], message1[2], message1[3], message2[3], ships_distance]
                result.append(flagged)
        else:
            if region_reached:
                return  #data is ordered by region, so first miss means we are out of the region
    return

# for given partition flag any meetings
# data is partitioned by time, 1 minute of messages per partition
def flag(partition):
    partition2 = []
    for message in partition:
        partition2.append(message)
    result = []
    for message1 in partition2:
        region1 = message1[4]
        y1 = message1[1]
        x1 = message1[2]
        check_distances(partition2, region1, y1, x1, message1, result)
    return result

# format: yyxx;
# possible values of yy between 01 and 18, possible values of xx between 01 and 36
def compute_region(y,x):
    region = 9999

    if y<0:
        y = y*(-1)
        y = (((int(y / 10) + 1) * 10) -10)*(-1)
    else:
        y = (int(y / 10) + 1) * 10
    if x<0:
        x = x*(-1)
        x = (((int(x / 10) + 1) * 10) -10)*(-1)
    else:
        x = (int(x / 10) + 1) * 10

    shifted_y = (y+90)/10
    shifted_x = (x+180)/10
    region = int(str(shifted_y).zfill(2) + str(shifted_x).zfill(2))

    return region


# check for invalid values, round timestamp to closest minute, return only [time, y, x, mmsi, region]
def filter(line):
    speed = 0.0
    x = 0
    y = 0
    time = 0
    mmsi = 'empty'
    region = 0
    filtered_message = ['empty', 0, 0, 'empty', 0]

    if line == 'empty':
        return filtered_message

    observation_list = line.split(", ")
    for i in observation_list:
        if i.startswith("u'sog'"):
            sog = i.split(": ")
            speed = sog[1]
        if i.startswith("u'timestamp'"):
            timestamp = i.split(": ")
            time = int(re.sub("[^0-9]", "", timestamp[1]))
            if time > 1:
                time = time // 10 ** (int(math.log(time, 10)) -9)
                time -= time % 60
            else:
                return filtered_message
        if i.startswith("u'y'"):
            y_coordinate = i.split(": ")
            y = float(y_coordinate[1])
        if i.startswith("u'x'"):
            x_coordinate = i.split(": ")
            x = float(x_coordinate[1])
        if i.startswith("u'mmsi'"):
            mmsi1 = i.split(": ")
            mmsi =int(re.sub("[^0-9]", "", mmsi1[1]))

    if (-180<x<180 and -90<y<90 and x!=0 and y!= 0 and mmsi != 'empty'):
            region = compute_region(y,x)
            filtered_message = [speed, time, y, x, mmsi, region]
    return filtered_message

# decode one message
def decode_line(line, timestamp):
    decoded = 'empty'

    try:
        line_list = line.split(",")
        field_0 = line_list[0]

        if(field_0[0] != "!"):
                # timestamp is present
                timestamp = field_0[:17]

        # the message to be decoded is in the 5th field of the list
        # field 6 contains information on the type; types starting with 0 can be successfuly decoded
        field_6 = line_list[6].rstrip('\r\n')

        if (str(field_6)[0] == "0"):
            decoded_without_timestamp = str((ais.decode(str(line_list[5]), 0)))
            ## insert current timestamp value
            timestamp_position = (decoded_without_timestamp).find("timestamp") + 12
            decoded = decoded_without_timestamp[:timestamp_position] + (timestamp) + decoded_without_timestamp[timestamp_position+3:]
        else:
            pass
            # message type invalid
    except Exception:
        pass
    return decoded, timestamp


# loop through lines of a partition and decode
def decode_loop(lines):
    decoded = []
    timestamp = 0
    for line in lines:
        line, timestamp = decode_line(line, timestamp)
        decoded.append(line)
    return decoded

###################################################################################################
#                                       main                                                      # 
###################################################################################################

# 4 directories to be filled in, replace "directory" with path to files/output
cores = 30

file_list = []
for filename in sorted(os.listdir("directory")):
    if filename.endswith('.txt'):
        file_list.append(filename)

minutes = len(file_list)
chunk = "directory"
full = False
counter = 0
times = []

while counter < minutes:
    if full == False:
        if counter == 0 or counter%cores == 0:
            chunk = chunk  + str(file_list[counter])
        else:
            chunk = chunk + "," + str(file_list[counter])
        if (counter+1)%cores == 0:
            full = True
        counter = counter + 1
    else:
        print "Starting analysis for 30 min of messages"
        start_time = time.time()
        sc =  SparkContext.getOrCreate()
        # eat .txt files in current directory, 
        # limited to 30 files at the time == 30 minutes (32 cores in the machine), may be changed by modifying cores variable
        inputRDD = sc.textFile(chunk)
        #input_size = inputRDD.count()

        # new RDD with decoded ais messages
        decodedRDD = inputRDD.mapPartitions(lambda partition: decode_loop(partition))
        #decoded_size = decodedRDD.count()

        filteredRDD = decodedRDD.map(lambda line: filter(line)) \
        .filter(lambda line: 'empty' not in line)
        #filtered_size = filteredRDD.count()

        spark = SparkSession(sc)
        hasattr(filteredRDD, "toDF")

        df = filteredRDD.toDF(['speed','time', 'y', 'x', 'mmsi', 'region'])

        df = df.filter(df.time != '0').filter(df.speed != '0.0').dropDuplicates(['time', 'mmsi'])
        df = df.drop('speed')
        #after_duplicate_removal = df.count()

        regionPartitions = df.repartition('time').sortWithinPartitions('region')

        rdd = regionPartitions.rdd.map(tuple)
        flaggedRDD = rdd.mapPartitions(lambda partition: flag(partition))

        flagged = flaggedRDD.toDF(['time', 'y', 'x', 'mmsi1', 'mmsi2', 'distance']).dropDuplicates(['mmsi1', 'mmsi2'])

        name = "directory" + str(counter)
        flagged.rdd.repartition(1).saveAsTextFile(name)
	#flagged_num = flagged.count()
        #flagged.show(100)

        end_time = time.time()
        #print("Read messages: " + str(input_size))
        #print("Decoded messages: " + str(decoded_size))
        #print("Messages after filtering: " + str(filtered_size))
        #print("Messages after duplicate removal / speed check: " + str(after_duplicate_removal))
	#print("Flagged messages: " + str(flagged_num))
        print("Time elapsed: " + str(end_time - start_time))
        #sleep(10)
        times.append(str(end_time - start_time))
        full = False
        chunk = "directory"
#print times

