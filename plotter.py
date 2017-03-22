#!/bin/python

import re
import os
import sys
import glob
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np

def read_amap(fn_in):
	lat = []
	lon = []
	with open(fn_in) as fo:
		for line in fo:
			#Cons.P(line)
			# {"loc": "37.6921876171,-89.8521296637", "size": 2, "uid": 1, "time": 2}
			mo = re.match(r"{\"loc\": \"(?P<lat>(\d|\.|-)+),(?P<lon>(\d|\.|-)+).+", line)
			#if mo is None:
			#	print "Error parsing line: ", line
			#	raise RuntimeError("Unexpected")
			#Cons.P("%s %s" % (mo.group("lon"), mo.group("lat")))
			loc = line.strip().split(':')[1].split(',')
			lat.append(float( loc[0].strip()[1:]))
			lon.append(float( loc[1].strip()[:-1]))
			#loc = (line.split(':')).split(',')
			#print loc
			#lon.append(float(mo.group("lon")))
			#lat.append(float(mo.group("lat")))
	return lon, lat
 
#map = Basemap(projection='merc', lat_0 = 57, lon_0 = -135,resolution = 'h', area_thresh = 0.1, llcrnrlon=-136.25, llcrnrlat=56.0,urcrnrlon=-134.25, urcrnrlat=57.75)

def plotonmap(lons, lats, amap):
	
	map = Basemap(width=12000000,height=9000000,projection='lcc',resolution='c',lat_1=45.,lat_2=55,lat_0=50,lon_0=-107.)

#Basemap(llcrnrlon=-119,llcrnrlat=22,urcrnrlon=-64,urcrnrlat=49,width=12000000,height=9000000,projection='lcc',resolution='c',lat_1=32.,lat_2=45.,lat_0=50,lon_0=-95.)
#Basemap(llcrnrlon=-119,llcrnrlat=22,urcrnrlon=-64,urcrnrlat=49,projection='lcc',lat_1=32,lat_2=45,lon_0=-95)

	map.drawcoastlines()
	map.drawcountries()
	map.drawmapboundary()
	map.drawstates()
	x,y = map(lons, lats)
	map.scatter(x,y, color='red', marker='o')

	#plt.show()
	plt.savefig("vamaps/"+amap+".pdf")

def main(argv):

	#files = os.listdir("amaps")
	amaps = glob.glob("amaps/*")
	#= [f for f in os.listdir("amaps") if os.path.isfile(os.path.join("amaps", f))]
	
	for amap in amaps: 
		print amap
		lons,lats = read_amap(amap)
		print len(lons), len(lats)
		plotonmap(lons, lats, amap)


if __name__ == "__main__":
	sys.exit(main(sys.argv))
