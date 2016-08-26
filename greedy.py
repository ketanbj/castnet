# This file implement simplified greedy replication algorithm.

# Python import
import random
import os
import sys
import time

# Project imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), 'aggregator'))
import util

class Greedy:

  def __init__(self, imap, amap):
	self.amap = amap
	self.imap = imap
	self.client_set = set(imap) # [client_ip, ]
	self.server_set = amap # [server_ip, ]
	self.content_set = set([]) # [uuid, ]
	self.access_map = {} # {uuid: {client_ip: num_request}}
	self.replica_map = {} # {uuid: {server_ip: num_replica}}
	self.last_timestamp = 0 # the timestamp of last update
	self.requests_per_replica = 3
	self.uuid_to_server = None
    # self.sample_interval = 1000 # the time interval between two rounds in second

  # update client_set, server_set, content_set, access_info
  # and replication status
  # call this function before running greedy algorithm
  def update(self):
    # clear data
    self.content_set = set([])
    self.access_map = {}
    self.replica_map = {}
    # update content_set, replica_map
    #for server in self.imap:
    #  file_list = util.get_file_list_on_server(server)
    #  for file_uuid in file_list:
    #    self.content_set.add(file_uuid)
    #    if file_uuid not in self.replica_map:
    #      self.replica_map[file_uuid] = {}
    #    if server not in self.replica_map[file_uuid]:
    #      self.replica_map[file_uuid][server] = 0
    #    self.replica_map[file_uuid][server] += 1
	# content_set
 	#content_list = []
    #for req in self.amap:
	#	content_list = req['uid']
		
	#self.content_set = set(visited)

	#replica_map
	

    current_timestamp = int(time.time())
    # used recently generated logs to update inner data structure
    for reqs in imap:
		timestamp = req['time'] 
		uuid = req['uid']
		source = req['loc'] 
		#source_uuid = req['dep']
		#dest, 
		#req_type, 
		#status, 
		#response_size
		if uuid not in self.content_set:
			continue
		self.content_set.add(uuid)
		if uuid not in self.access_map:
			self.access_map[uuid] = {}
		self.client_set.add(source)
		if req_type == 'READ':
			if source not in self.access_map[uuid]:
				self.access_map[uuid][source] = 0
				self.access_map[uuid][source] += 1
		self.last_timestamp = current_timestamp

  def execute(self):
    self.update()
    request_delta = self.requests_per_replica / 10
    replica_delta = 1
    i = 0
    if not self.enough_replica_on_increase(request_delta):
      self.add_replica(request_delta, replica_delta)
    # currently we don't remove any replica
    # else:
      # remove_replica()
      
  # test whether current replicas can handle more requests
  #
  # delta: specify the amount of request increased every time 
  def enough_replica_on_increase(self, delta):
    for c in self.content_set:
      if c in self.access_map:
        for a in self.access_map[c].keys():
          # add a small amount of requests for content c from client a
          self.access_map[c][a] += delta
          # test whether current replicas can handle that much request
          is_enough = self.enough_replica()
          # back tracking,
          self.access_map[c][a] -= delta
          if not is_enough:
            return False
    return True

  def add_replica(self, request_delta, replica_delta):
    I = []
    for c in self.content_set:
      if c in self.access_map:
        for a in self.access_map[c].keys():
          # add a small amount of requests for content c from client a
          self.access_map[c][a] += request_delta
          # test whether current replicas can handle that much request
          if not self.enough_replica():
            I.append((a,c))
          # back tracking,
          self.access_map[c][a] -= request_delta
    max_satisfied_num = 0
    best_c = None
    best_s = None
    # find the server s to replicate content c so that  
    # maximum number of starved clients can be satisfied
    for a, c in I:
      for s in self.server_set:
        satisfied_num = 0
        self.access_map[c][a] += request_delta
        if s not in self.replica_map[c]:
          self.replica_map[c][s] = 0
        self.replica_map[c][s] += replica_delta
        if self.enough_replica():
          satisfied_num += 1
        self.access_map[c][a] -= request_delta
        self.replica_map[c][s] -= replica_delta
        if self.replica_map[c][s] == 0:
          self.replica_map[c].pop(s)
        if (satisfied_num > max_satisfied_num):
          max_satisfied_num = satisfied_num
          best_c = c
          best_s = s
    if max_satisfied_num > 0:
      source = self.replica_map[best_c].iterkeys().next()
      if source == best_s:
        # can't hold more than 1 replica, replicate to a random other server
        best_s = random.sample(self.server_set - set([source]), 1)[0]
      self.replicate(best_c, source, best_s)
    else:
      # replicate everything
      print 'replicate to all servers'
      for content in self.content_set:
        if not self.enough_replica_for_content(content):
          if content not in self.replica_map:
            continue
          source = self.replica_map[content].iterkeys().next()
          #select first none zero replica
          for server in self.server_set:
            print 'replicate ' + 'content: ' + content + ' from: ' + source + ' to ' + server
            util.replicate(content, source, server)

  def enough_replica(self):
    # this is an approximate implementation, may need to
    # construct a bipartite graph and run min matching algo
    for c in self.content_set:
      if not self.enough_replica_for_content(c):
        return False
    return True
  
  def enough_replica_for_content(self, c):
    # this is an approximate implementation, may need to
    # construct a bipartite graph and run min matching algo
    server_to_request_sum_map = {}
    if c not in self.access_map.keys():
      # no client accesses c
      return True
    for a in self.access_map[c].keys():
      nearest_server = util.find_closest_servers_with_ip(a, self.server_set)[0]['server']
      if nearest_server not in server_to_request_sum_map:
        server_to_request_sum_map[nearest_server] = 0 
      server_to_request_sum_map[nearest_server] += self.access_map[c][a]
    for server, request_sum in server_to_request_sum_map.iteritems():
      if (c not in self.replica_map) or (server not in self.replica_map[c]):
        return False
      if self.replica_map[c][server] * self.requests_per_replica < request_sum:
        return False
    return True

  def replicate(self, content, source, dest):
    print 'Greedy: replicate file %s from %s to %s', (content, source, dest)
    if source == dest:
      if dest not in self.replica_map[content]:
        self.replica_map[content] = 0
      self.replica_map[content][dest] += 1
    else:
      util.replicate(content, source, dest)

