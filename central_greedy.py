# This file implement simplified greedy replication algorithm.

import random
import os
import sys
import time
import geopy
from geopy.distance import great_circle
import util

class CentralizedGreedy:

  def __init__(self, imap, amap, level=1):
	self.amap = amap # [server_ip, ]
	self.imap = imap
	self.replica_map = {} # { file_uuid: servers that store the file }
	self.last_timestamp = 0 # the time stamp of last update
	self.level = level
  
  def update(self):
    # update inner data
	return

  # Gets sort key for sort function to sort by ascending distance
  def get_distance_key(self, server_dict):
    return server_dict['distance']

  # Finds the closest server for a lat/long tuple pair
  #
  # params:
  #   location: lat/long tuple
  #   servers_to_search: a list of servers to search. Uses self.servers by      default.
  # returns: list of closest to furthest, where each item is a dict with        `server` and `distance`
  def find_closest_servers(self, location, servers_to_search = None):
	if servers_to_search is None:
		servers_to_search = self.imap

	best_servers = []

	for server in servers_to_search:
		server_dict = { 'server': server, 'distance': None }
		item_location = geopy.Point(location[0], location[1])
		server_lat_lon = (server.split(',')[0],server.split(',')[1])
		if server_lat_lon is None:
			raise ValueError('Server <' + server + '> latitude/longitude could not  be found!')
		server_location = geopy.Point(server_lat_lon[0], server_lat_lon[1])
		server_dict['distance'] = great_circle(item_location, server_location).km
		best_servers.append(server_dict)

	best_servers.sort(key=self.get_distance_key)
	return best_servers

  def process_deps(self, dep_map, level):
	if level < 1:
		return
	#print "Interation no. for dependency .... ",level
	for req_list in dep_map.values():
		for req in req_list:
			latitude = float(req['loc'].split(',')[0])
			longitude = float(req['loc'].split(',')[1])
			location = (latitude, longitude)

			best_server = self.find_closest_servers(location)[0]
			self.replica_map[req['uid']] = best_server
		
			ddep_map = {}
			#FIXME: handle creating dependency map for multiple level of dependencies
			#if req['uid'] not in ddep_map.keys():
			#		ndep = []
			#		ndep.append(re)
			#		ddep_map[re['uid']] = ndep
			#	else:
			#		ddep_map[re['uid']].append(re)

        self.process_deps(ddep_map, level-1)


  def execute(self):
	self.update()
	for req in self.amap:
		latitude = float(req['loc'].split(',')[0])
		longitude = float(req['loc'].split(',')[1])
		location = ((latitude, longitude))
		
		best_server = self.find_closest_servers(location)[0]
		self.replica_map[req['uid']] = best_server

		dep_map = {}
		#create dependency map
		if req['uid'] not in dep_map.keys():
			ndep = []
			ndep.append(req)
			dep_map[req['uid']] = ndep
		else:
			dep_map[req['uid']].append(req)

		self.process_deps(dep_map, self.level)
	#print self.replica_map
      #self.replicate(file_uuid, source, target)

  def replicate(self, content, source, dest):
    print 'Greedy: replicate file %s from %s to %s', (content, source, dest)
    #if source == dest:
      # a server can have at most one replcia, se we replicate to second nearest server
      #dest_simulation_ip = util.convert_to_simulation_ip(target)
      #candidate_servers = self.server_set - set(self.replica_map[content])
      #dest = util.find_closest_servers_with_ip(dest_simulation_ip, candidate_servers)[0]['server']
    #util.replicate(content, source, dest)

