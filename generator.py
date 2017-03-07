#!/bin/python

import numpy as np
import sys
import json
import matplotlib.pyplot as plt
#import scipy.special as sps

class Generator:

	def __init__(self, imap):
		self.imap = imap
		self.debug = False
		self.zipf_distr_param = 1.5
		self.normal_loc_t = 43200
		self.normal_scale_t = 10800

	def ticking(self):
		sys.stdout.write("/")
		sys.stdout.flush()
		sys.stdout.write("\r")
		sys.stdout.flush()
		sys.stdout.write("-")
		sys.stdout.flush()
		sys.stdout.write("\r")
 		sys.stdout.flush()
		sys.stdout.write("\r")
		sys.stdout.flush()
 		sys.stdout.write("|")
		sys.stdout.flush()
		sys.stdout.write("-")
		sys.stdout.flush()
		sys.stdout.write("\r")
 		sys.stdout.flush()

	def create_random_point(self, str_loc):
		distance = 35405. #typical range of cell tower
		x0 = float(str_loc.split(',')[0])
		y0 = float(str_loc.split(',')[1])
		#print x0
		#print y0
		r = distance/ 111300
		#print r
		u = np.random.uniform(0,1)
		v = np.random.uniform(0,1)
		w = r * np.sqrt(u)
		t = 2 * np.pi * v
		x = w * np.cos(t)
		x1 = x / np.cos(y0)
		y = w * np.sin(t)
		#print x
		#print y
		res =  str(x0+x1)+','+ str(y0+y)
		#print res
		return res

	def show(self,s, dist):
		if dist == 'random':
			x = range(0,len(s))
			plt.scatter(x, s)
			plt.show()
		elif dist == 'gaussian':
			mu = 4
			sigma = 0.5
			count, bins, ignored = plt.hist(s,100)
			plt.plot(bins, 1/(sigma * np.sqrt(2 * np.pi)) * np.exp( - (bins - mu)**2 / (2 * sigma**2) ),linewidth=2, color='r')
			plt.show()
		elif dist == 'zipfian':
			#print "Some issue with scipy import"
			return
			#a =2
			#count, bins, ignored = plt.hist(s[s<50], 50)
			#x = np.arange(1., 50.)
			#y = x**(-a)/sps.zetac(a)
			#plt.plot(x, y/max(y), linewidth=2, color='r')
			#plt.show()

	def generate_amap(self, sd,sf,td,tf,zd,zf,dd,df,rd,rf):

		l_imap = len(self.imap)
		#print "Number of i locations: ", l_imap
		amap = []		
		total_requests = int(sf)*l_imap
		unique_requests = total_requests/int(rf)
		if(self.debug):
			print "Generating for conf: "
			print "Spatial: " + sd +'_' + sf
			print "Temporal: "+ td +'_' + tf
			print "Size: "+zd +'_' + zf
			print "Dependency: "+dd +'_' + df
			print "Redundancy: "+rd +'_' + rf

		loc_indexes = []
		if sd == 'random':
			loc_indexes = np.random.randint(0,l_imap, total_requests)
		elif sd == 'gaussian':
			loc_indexes = np.random.normal(l_imap/2,l_imap/20,total_requests)
			loc_indexes = loc_indexes.astype(np.int64) #np.remainder(loc_indexes, l_imap)
		elif sd == 'zipfian':
			loc_indexes = np.random.zipf(self.zipf_distr_param,total_requests)
			loc_indexes = np.remainder(loc_indexes,l_imap-1)
			loc_indexes = loc_indexes.astype(np.int64) #np.remainder(loc_indexes, l_imap)
			#print loc_indexes
		else:
			print "Unsupported spatial distribution"
			exit()
		
		uuids_set = set(loc_indexes)
		print "nb locations: ", len(loc_indexes)
		print "unique locations: ", len(uuids_set)
		uuids = range(0,unique_requests)
		uids = []
		print "uuids ", len(uuids)
		if rd == 'random':
			uids = np.random.randint(0,unique_requests, total_requests)
		elif rd == 'gaussian':
			uids = np.random.normal(unique_requests/2,unique_requests/20,total_requests)
			uids = uids.astype(np.int64) #np.remainder(loc_indexes, l_imap)
			#offset = abs(np.min(uuids))
			#uuids = np.add(uuids, offset)
		elif rd == 'zipfian':
			uids = np.random.zipf(self.zipf_distr_param, total_requests)
			uids = np.remainder(uids,unique_requests)
			loc_indexes = uids.astype(np.int64) #np.remainder(loc_indexes, l_imap)
		else:
			print "Unsupported redundancy dstribution"
			exit()
		print "uids: ", len(uids)

		timestamps = []
		if td == 'random':
			timestamps = np.random.randint(0, 86400, total_requests)
		elif td == 'gaussian':
			timestamps = np.random.normal(self.normal_loc_t, self.normal_scale_t, total_requests)
			#timestamps = np.remainder(timestamps, 86400)
		elif td == 'zipfian':
			timestamps = np.random.zipf(self.zipf_distr_param, total_requests)
			timestamps = np.remainder(timestamps, 86400) 
		else:
			print "Unsupported temporal distribution"	
			exit()
		print "timestamps: ",len(timestamps)

		
		sizes = []
		if zd == 'random':
			sizes = np.random.randint(1,int(zf), unique_requests)
		elif zd == 'gaussian':
			sizes = np.random.normal(int(zf)/2,int(zf)/20,unique_requests)
			sizes = sizes.astype(np.int64) #np.remainder(loc_indexes, l_imap)
			#offset = abs(np.min(sizes))
			#sizes = np.add(sizes, offset+1)
			#sizes = np.remainder(sizes,int(zf))
		elif zd == 'zipfian':
			sizes = np.random.zipf(self.zipf_distr_param, unique_requests)
			sizes = np.remainder(sizes,int(zf))
			#sizes = np.add(sizes,1)
		else:
			print "Unsupported size dstribution"
			exit()
		print "sizes: ",len(sizes)
		
#		deps_index = []
#		if dd == 'random':
#			deps_index = np.random.randint(0,unique_requests, unique_requests)
#		elif dd == 'gaussian':
#			deps_index = np.random.normal(1,1,unique_requests)
#			offset = abs(np.min(deps_index))
#			deps_index = np.add(deps_index, offset)
#		elif dd == 'zipfian':
#			deps_index = np.random.zipf(self.zipf_distr_param, unique_requests)
#			deps_index = np.remainder(deps_index, unique_requests)
#		else:
#			print "Unsupported dependency dstribution"
#			exit()
		#print timestamps		
		assigned = {}
		for i in range(0, total_requests):
			line = {}
			#print i % l_imap
			#print l_imap
			line['loc'] = self.create_random_point(self.imap[int(loc_indexes[i])])

			line['time'] = int(timestamps[i])
			
			line['uid'] = int(uids[i])
			#if uuids[i % unique_requests] in assigned.keys():
			try:
				line['size'] = assigned[uids[i]][0]
				#print assigned[uuids[i % unique_requests]][0], i % unique_requests
#				line['dep'] = assigned[uuids[i % unique_requests]][1]
			#else:
			except Exception, e:
				#print repr(e)
				#print sizes[i % unique_requests], i % unique_requests
				line['size'] = sizes[i % unique_requests]
				assigned[uids[i]] = (sizes[i % unique_requests], uids[i])
				#print "In exception"
			#else:
			#	line['size'] = int(sizes[i % unique_requests])
#				line['dep'] = uuids[int(deps_index[i % unique_requests])]
			#print line['uid']," : ",line['size']
			amap.append(line)

#		for line in amap:
#			if line['dep'] not in uuids:
#				print "You messed up something - dependency is not another request"

		self.dump_amap(amap, sd,sf,td,tf,zd,zf,dd,df,rd,rf)		

		# add uids & size
		#self.ticking()
		return amap

	def dump_amap(self, amap, sd,sf,td,tf,zd,zf,dd,df,rd,rf):
		dump_name = 'amaps/amap_' + sd[0]+sf+td[0]+tf+zd[0]+zf[0]+dd[0]+df+rd[0]+rf
		
		with open(dump_name, 'wt') as amap_dump:
			for line in amap:
				json.dump(line,amap_dump)
				amap_dump.write("\n")
			#json.dump(amap,amap_dump,indent=0, cls=NoIndentEncoder)
		#print "written amap in ", dump_name


