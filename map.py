import gzip
import sys
import ais
import os
import time

from mpl_toolkits.basemap import Basemap
import numpy as np
import matplotlib
matplotlib.use('tkagg')         # to show images over ssh, run ssh -X
import matplotlib.pyplot as plt

decoded_observations = 0
all_observations = 0
file_counter = 0
plot_counter = 0

print ("Creating a map")

map = Basemap(projection='merc', llcrnrlat=-80, urcrnrlat=80, llcrnrlon=-180,urcrnrlon=180,lat_ts=20, resolution='i')
map.drawcoastlines()
#map.shadedrelief()
map.drawparallels(np.arange(-90.,91.,30.))
map.drawmeridians(np.arange(-180.,181.,60.))
plt.title("Observations map")

print ("Map created")

def plot_on_map(observation):
        global map
        global plot_counter
        x = 0
        y = 0
        #print(observation)
        observation_list = observation.split(", ")
        for i in observation_list:
                if i.startswith("u'y'"):
                        y_coordinate = i.split(": ")
                        y = float(y_coordinate[1])
                        #print y
                if i.startswith("u'x'"):
                        x_coordinate = i.split(": ")
                        x = float(x_coordinate[1])
                        #print x
        if (x != 0 and y != 0):
                lon, lat = map(x,y)
                map.plot(lon,lat,'ro', markersize = 4)
                plot_counter = plot_counter + 1

def decode(filename):

        file  = open(filename, 'r')
        global decoded_observations
        global all_observations
        timestamp = '0'

        for line in file:
                all_observations = all_observations +1
                try:
                        line_list = line.split(",")

                        field_0 = line_list[0]
                        if(field_0[0] != "!"):
                                # timestamp is present
                                timestamp = field_0[:17]
                        field_6 = line_list[6].rstrip('\r\n')
                        if (str(field_6)[0] == "0"):
                                decoded = str((ais.decode(str(line_list[5]), 0)))
                                ## insert current timestamp value
                                timestamp_position = (decoded).find("timestamp") + 12
                                decoded_timestamp = decoded[:timestamp_position] + str(timestamp) + decoded[timestamp_position+3:]
                                if (all_observations%1 == 0): #specify if want to only run through a part
                                        plot_on_map(decoded_timestamp)
                                        decoded_observations = decoded_observations+1
                        else:
                                file.readline()      # skip next line as it is a part of previous observation
                except Exception:
                        pass

        print ("Successfully decoded observations: " + str(decoded_observations) +"/" + str(all_observations) + " (" + str(float(decoded_observations)/float(all_observations)) + ")")

start = time.time()

for filename in os.listdir(os.getcwd()):
        if filename.endswith(".txt"):
                file_counter = file_counter+1
                print ("decoding file " + str(file_counter) +": " + filename )
                decode(filename)
                continue
        else:
                continue

end = time.time()
print("Time elapsed: " + str(end - start))
print ("Successfully decoded observations: " + str(decoded_observations) +"/" + str(all_observations) + " (" + str(float(decoded_observations)/float(all_observations)) + ")")
print ("Observations plotted on the map: " + str(plot_counter))
plt.show()
#plt.savefig('map.pdf')


