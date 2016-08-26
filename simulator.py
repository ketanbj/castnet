#!/bin/python

import argparse
import time
import ConfigParser
import numpy as np
import pickle
import json
import pprint as pp


from generator import Generator
from volley import Volley
from greedy import Greedy
from central_greedy import CentralizedGreedy
import greedy
import distributed
import castnet

verbose = True
debug = True

start_time = 0
end_time = 0

def load_imap(imap):
	r_imap = []
	with open(imap) as imap:
		for line in imap:
			r_imap.append(line.strip())
		return r_imap
	
def load_amap(amap):
	r_amap = []
	r_amaps = []
	with open(amap,'rt') as amap:
		r_amap = json.load(amap)
	#amap.read()
	#r_map = json.loads(json_data)
	print "Read number of reqs: ", len(r_amap)
	r_amaps.append(r_amap)
	return r_amaps

def ConfigSectionMap(config, section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def create_amaps(amap,imap):

	amaps = []

	access_config = ConfigParser.ConfigParser()
	access_config.read(amap)	
	sections = access_config.sections()
	
	print len(sections)
	#FIXME: automate for different sections
	workload_generator = Generator(imap)
	for section in sections:
		section_options = ConfigSectionMap(access_config, section)
		sd = section_options['spatial_distribution']
		sf = section_options['spatial_factor']
		td = section_options['temporal_distribution']
		tf = section_options['temporal_factor']
		zd = section_options['size_distribution']
		zf = section_options['size_factor']
		dd = section_options['dependency_distribution']
		df = section_options['dependency_factor']
		rd = section_options['redundancy_distribution']
		rf = section_options['redundancy_factor']
		
		r_amap = workload_generator.generate_amap(sd,sf,td,tf,zd,zf,dd,df,rd,rf)
		amaps.append(r_amap)
	return amaps

def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('--algo', choices=['volley', 'greedy', 'cgreedy', 'distributed', 'castnet'], help='the algorithm used for replication', required=True)
	parser.add_argument('--imap', help='File containing imap', required=True)
	parser.add_argument('--amap', help='File containing amap or amap conf params', required=True)
	args = vars(parser.parse_args())
	return args

def dump_amap(amap, index):
	dump_name = 'amaps/map_' + str(index)
	with open(dump_name, 'wt') as amap_dump:
		json.dump(amap,amap_dump)
	print "written amap in ", 'amap_' + str(index)

if __name__ == '__main__':
	args = parse_args()
	if debug:
		print args

	imap = load_imap(args['imap'])

	if '.ini' in args['amap']:
		amaps = create_amaps(args['amap'], imap)
		count = 0
		for amap in amaps:
			dump_amap(amap, count)
		count = count + 1
	else:
		amaps=load_amap(args['amap'])
		print "Loaded amap !"

	for amap in amaps:
		print "Number of requests in amap: ", len(amap)
		#print amap
		algo = None
		if args['algo'] == 'volley':
			print "Using Volley algorithm ..."
			algo = Volley(imap, amap)
		elif args['algo'] == 'greedy':
			print "using greedy algorithm ..."
			algo = Greedy(imap, amap)
		elif args['algo'] == 'cgreedy':
			print "using centralized greedy algorithm ..."
			algo = CentralizedGreedy(imap, amap)
		elif args['algo'] == 'distributed':
			print "using distributed algorithm ..."
			algo = Distributed(imap, amap)
		elif args['algo'] == 'castnet':
			print "Using castnet algorithm ..."
			algo = Castnet(imap, amap, budget)
		else:
			print "Don't know how we reached here"

		start_time = int(time.time())
		algo.execute()
		end_time = int(time.time())
		print "Execution time: ", end_time - start_time
