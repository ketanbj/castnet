# Implementation of Volley
import geopy
import json
import math
import os
import requests
import sqlite3
import sys
import time
import urllib
import numpy as np
from geopy.distance import great_circle

# Project Imports
up_one_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
sys.path.insert(0, up_one_dir)
sys.path.insert(0, os.path.join(up_one_dir, 'cache'))
sys.path.insert(0, os.path.join(up_one_dir, 'aggregator'))
import util

# Configurable Constants
INTERDEPENDENCY_ITERATIONS = 5
KAPPA = 0.5

class Castnet:

  def __init__(self, imap, amap):
	self.full_imap = imap
	self.amap = amap
	self.uuid_metadata = {}
	self.imap = []

  # Execute Volley algorithm
  def execute(self, budget):
	print "Castnet.execute() <<"	
	print "------------------------------------"
	print "Select infrastructure nodes"
	self.imap = self.cast_net()	
	print "------------------------------------"
	print "Initial placement Phase 1"
	locations_by_uid = self.throw()
	print locations_by_uid
	print "------------------------------------"
	print "Phase 2 placement"
	locations_by_uid = self.reduce_latency(locations_by_uid)
	print locations_by_uid
	print "------------------------------------"
	print "Phase 3 final placement"
	placements_by_server = self.sink(locations_by_uid)
	print placements_by_server
	print "------------------------------------"
	self.migrate_to_locations(placements_by_server)
	print 'Castnet execute() >>'

  def castnet(self, budget):
	r_imap = []
	# Find subset of infratructure to create the castnet
	
	return r_imap

  # PHASE 1: Compute Initial Placement
  def throw(self):

    # Unique requests
	locations_by_uid = {}
	size_by_uid = {}

	visited = []
	for req in self.amap:
		if req['uid'] in visited:
			latitude = float(req['loc'].split(',')[0])
			longitude =  float(req['loc'].split(',')[1])
			locations_by_uid[req['uid']].append((latitude, longitude))
			size_by_uid[req['uid']].append(int(req['size']))
		else:
			n_loclist = []
			n_sizelist = []
			latitude = float(req['loc'].split(',')[0])
			longitude =  float(req['loc'].split(',')[1])
			n_loclist.append((latitude, longitude))
			n_sizelist.append(int(req['size']))
			locations_by_uid[req['uid']] = n_loclist
			size_by_uid[req['uid']] = n_sizelist
			visited.append(req['uid'])
	
	#print len(size_by_uid), len(locations_by_uid)	
	for key in locations_by_uid.keys():
		locations_by_uid[key] = self.weighted_spherical_mean_helper(sum(size_by_uid[key]), size_by_uid[key],locations_by_uid[key])
	#print locations_by_uid
	return locations_by_uid

  # PHASE 2: Iteratively Move Data to Reduce Latency
  def adjust(self, locations_by_uid):

    for i in range(INTERDEPENDENCY_ITERATIONS):
		uuid_to_interdependencies = []
		for uid, location in locations_by_uid.items():
		# Find a count for unique uuids a uuid is interdependent with, and how many requests for each interdependent request are made
			uid_count = 0
			visited = []
			req_count = 0
			for req in self.amap:
				if uid == req['dep']:
					if uid not in visited:
						uid_count = uid_count +1
						visited.append(uid)
				req_count = req_count +1

			uuid_to_interdependencies.append( (uid_count, req_count))
			#self.log_manager.get_interdependency_grouped_by_uuid(uuid)

		for tuple in uuid_to_interdependencies:
			other_item_uuid = tuple[0]
			other_item_location = locations_by_uid[other_item_uuid]
			request_count = tuple[1]
			distance = util.get_distance(location, other_item_location)
			weight = 1 / (1 + (KAPPA * distance * request_count))
			location = self.interp(weight, location, other_item_location)

		locations_by_uid[uid] = location

    return locations_by_uid

  # PHASE 3: Iteratively Collapse Data to Datacenters
  def sink(self, locations_by_uid):
	placements_by_server = {}
	for server in self.imap:
		placements_by_server[server] = []

	for uid, location in locations_by_uid.iteritems():
		metadata = {'current_server': None, 'optimal_location': location, 'uuid': uid, 'dist': None, 'file_size': None, 'request_count': None}
		best_server = None
		
		metadata['current_server'] = location
		req_count = 0
		for req in self.amap:
			if req['uid'] == uid:
				metadata['file_size'] = req['size']
				req_count = req_count + 1
		metadata['request_count'] = req_count
		

		best_servers = self.find_closest_servers(location)
		best_server = best_servers[0]
		metadata['dist'] = best_server['distance']
		
		self.uuid_metadata[uid] = metadata
		placements_by_server[best_server['server']].append(uid)

		placements_by_server = self.redistribute_server_data_by_capacity(placements_by_server)
	return placements_by_server

  # PHASE 4: Call migration methods on each server
  def migrate_to_locations(self, placements_by_server):
	print "Implement actual migration here"
	return


  # PHASE 4: Call migration methods on each server
  def migrate_to_locations(self, placements_by_server):
	print "Implement actual migration here"
	return

  # Gets sort key for sort function to sort by ascending request_count
  def get_sort_key_by_request_count(self, uuid):
    return self.uuid_metadata[uuid]['request_count']

  # Gets sort key for sort function to sort by ascending distance
  def get_distance_key(self, server_dict):
    return server_dict['distance']

  # Finds the closest server for a lat/long tuple pair
  #
  # params:
  #   location: lat/long tuple
  #   servers_to_search: a list of servers to search. Uses self.servers by default.
  # returns: list of closest to furthest, where each item is a dict with `server` and `distance`
  def find_closest_servers(self, location, servers_to_search = None):
    if servers_to_search is None:
      servers_to_search = self.imap

    best_servers = []

    for server in servers_to_search:
      server_dict = { 'server': server, 'distance': None }
      item_location = geopy.Point(location[0], location[1])
      server_lat_lon = (server.split(',')[0],server.split(',')[1])
      if server_lat_lon is None:
        raise ValueError('Server <' + server + '> latitude/longitude could not be found!')
      server_location = geopy.Point(server_lat_lon[0], server_lat_lon[1])
      server_dict['distance'] = great_circle(item_location, server_location).km

      best_servers.append(server_dict)

    best_servers.sort(key=self.get_distance_key)

    return best_servers

  # Check capacity of server
  #
  # params:
  #   server: hostname of server to check
  def total_server_capacity(self, server):
	#FIXME: variable storage capacity to be loaded from imap
	return 5242880

  # Check capacity and redistribute data to each server
  #
  # params:
  #   placements_by_server: dictionary mapping server hostname -> set of uuids
  def redistribute_server_data_by_capacity(self, placements_by_server):
    space_remaining = {}
    servers_with_capacity = set()
    servers_over_capacity = set()

    for server in self.imap:
      space_remaining[server] = self.total_server_capacity(server)

      placements = placements_by_server[server]

      for uuid in placements:
        metadata = self.uuid_metadata[uuid]
        space_remaining[server] -= metadata['file_size']

      if space_remaining[server] > 0:
        servers_with_capacity.add(server)
      else:
        servers_over_capacity.add(server)

    for server in servers_over_capacity:
      placements = placements_by_server[server]
      placements.sort(key=self.get_sort_key_by_request_count)

      # move top uuids to nearest locations (or second-nearest, etc if other servers are full)
      # until server is no longer over capacity
      while space_remaining[server] < 0:
        uuid = placements.pop()
        metadata = self.uuid_metadata[uuid]
        best_servers = self.find_closest_servers(metadata['optimal_location'], servers_with_capacity)

        for i, best_server_info in enumerate(best_servers):
          best_server = best_server_info['server']
          if space_remaining[best_server] >= metadata['file_size']:
            space_remaining[best_server] -= metadata['file_size']
            placements_by_server[best_server].append(uuid)
            break
          if i == (len(best_servers) - 1):   # haven't found a server on the last iteration
            raise ValueError("There is too much data for the servers' storage capacity to handle.")

        space_remaining[server] += metadata['file_size']

        print 'PLACEMENTS: ' + str(placements)

    return placements_by_server

  # Convert from latitude to radians from the North Pole
  def convert_lat_to_radians(self, lat):
    # Subtract 90 to make range [-180, 0], then negate to make it [0, 180]
    degrees_from_north_pole = (lat - 90) * -1;

    return math.radians(degrees_from_north_pole)

  # Convert longitude to radians
  def convert_lng_to_radians(self, lng):
    # Add 180 to make range [0, 360]
    # lng = lng + 180
    return math.radians(lng)

  # Convert from radians from the North Pole to degrees
  def convert_lat_to_degrees(self, lat):
    degrees_from_north_pole = math.degrees(lat)
    degrees_from_equator = (degrees_from_north_pole * -1) + 90

    return degrees_from_equator

  # Convert longitude to degrees
  def convert_lng_to_degrees(self, lng):
    # lng = math.degrees(lng)
    # return lng - 180
    return math.degrees(lng)

  # Normalize radian values from [-Inf, Inf] to specified range - default = [0, 2pi)
  def normalize_radians(self, lng, min_val = 0, max_val = (2 * math.pi)):
    if math.fabs(max_val - min_val - (2 * math.pi)) >= 0.001:
      raise ValueError('Range for function must be around 2*pi.')
    while lng < min_val:
      lng = lng + (2 * math.pi)
    while lng >= max_val:
      lng = lng - (2 * math.pi)
    return lng

  # Find the average longitude of two points where longitude is given from [0, Inf]
  def find_avg_lng(self, lng_a, lng_b):
    # Need to normalize to [0, 2pi) so that arithmetic mean comes out to a reasonable value
    lng_a = self.normalize_radians(lng_a)
    lng_b = self.normalize_radians(lng_b)

    # Here, normalize to [-pi, pi) for longitude
    return self.normalize_radians((lng_a + lng_b) / 2, -1 * math.pi, math.pi)

  # Helper for weighted_spherical_mean, defined in Volley paper
  #
  # params:
  #   weight: weight for interpolation
  #   loc_a: lat/lng for location A
  #   loc_b: lat/lng for location B
  def interp(self, weight, loc_a, loc_b):
    lat_a = self.convert_lat_to_radians(loc_a[0])
    lng_a = self.convert_lng_to_radians(loc_a[1])
    lat_b = self.convert_lat_to_radians(loc_b[0])
    lng_b = self.convert_lng_to_radians(loc_b[1])

    d_first = math.cos(lat_a) * math.cos(lat_b)
    d_second = math.sin(lat_a) * math.sin(lat_b) * math.cos(lng_b - lng_a)
    d = math.acos(d_first + d_second)

    gamma_numerator = math.sin(lat_b) * math.sin(lat_a) * math.sin(lng_b - lng_a)
    gamma_denominator = math.cos(lat_a) - (math.cos(d) * math.cos(lat_b))
    gamma = math.atan2(gamma_numerator, gamma_denominator)

    beta_numerator = math.sin(lat_b) * math.sin(weight * d) * math.sin(gamma)
    beta_denominator = math.cos(weight * d) - (math.cos(lat_a) * math.cos(lat_b))
    beta = math.atan2(beta_numerator, beta_denominator)

    lat_c_first = math.cos(weight * d) * math.cos(lat_b)
    lat_c_second = math.sin(weight * d) * math.sin(lat_b) * math.cos(gamma)
    lat_c = math.acos(lat_c_first + lat_c_second)
    lat_c = self.convert_lat_to_degrees(lat_c)

    # Find an average of coming from either direction for antipodal nodes
    lng_c_1 = lng_b - beta
    lng_c_2 = lng_a + beta
    lng_c = self.find_avg_lng(lng_c_1, lng_c_2)
    lng_c = self.convert_lng_to_degrees(lng_c)

    return (lat_c, lng_c)

  # Recursive helper for weighted_spherical_mean
  #
  # params:
  #   weights: a list of weights
  #   locations: a list of latitude/longitude tuples for clients, has same cardinality as weights
  def weighted_spherical_mean_helper(self, total_weight, weights, locations):
    if len(weights) != len(locations):
      raise ValueError('Weights and locations must have the same length.')

    length = len(weights)

    # print '------ Weighted spherical mean -------- '
    # print 'Length: ' + str(length)
    # print 'WEIGHTS: ' + str(weights)
    # print 'LOCATIONS: ' + str(locations)

    current_weight = float(weights.pop())
    weight = current_weight / total_weight
    location = locations.pop()

    # print 'current_weight: ' + str(weight)

    if length == 1:
      return location

    return self.interp(weight, location, self.weighted_spherical_mean_helper(total_weight, weights, locations))

  # Find the weighted spherical mean location for a data item
  #
  # params:
  #   uuid: uuid of the data item
  def weighted_spherical_mean(self, location_by_uid, uid):
   
	weights = []
	locations = []

	total_weight = float(sum(weights))
	return self.weighted_spherical_mean_helper(total_weight, weights, locations)
