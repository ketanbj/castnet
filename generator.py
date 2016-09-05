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
		self.zipf_distr_param = 4

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
			mu = 1
			sigma = 1
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
		nb_uuids = (int(sf)*l_imap)/int(rf)
		if(self.debug):
			print "Generating for conf: "
			print "Spatial: " + sd +'_' + sf
			print "Temporal: "+ td +'_' + tf
			print "Size: "+zd +'_' + zf
			print "Dependency: "+dd +'_' + df
			print "Redundancy: "+rd +'_' + rf

		loc_indexes = []
		if sd == 'random':
			loc_indexes = np.random.randint(0,l_imap, int(sf) * l_imap)
		elif sd == 'gaussian':
			loc_indexes = np.random.normal(1,1,int(sf)*l_imap)
			offset = abs(np.min(loc_indexes))
			s_indexes = np.add(loc_indexes, offset)
		elif sd == 'zipfian':
			loc_indexes = np.random.zipf(self.zipf_distr_param,int(sf)*l_imap)
			loc_indexes = np.remainder(loc_indexes,l_imap-1)
			#print loc_indexes
		else:
			print "Unsupported spatial distribution"
			exit()

		show_distributions= False
		if(show_distributions):
			self.show(loc_indexes,sd)

		timestamps = []
		if td == 'random':
			timestamps = np.random.randint(0, 86400, int(sf)*l_imap)
		elif td == 'gaussian':
			timestamps = np.random.normal(1, 1, int(sf) * l_imap)
			offset = abs(np.min(timestamps))
			timestamps = np.add(timestamps, offset)
			timestamps = np.remainder(timestamps, 86400)
		elif td == 'zipfian':
			timestamps = np.random.zipf(self.zipf_distr_param, int(sf) * l_imap)
			timestamps = np.remainder(timestamps, 86400) 
		else:
			print "Unsupported temporal distribution"	
			exit()

		uuids = []
		if rd == 'random':
			uuids = np.random.randint(0,nb_uuids, nb_uuids)
		elif rd == 'gaussian':
			uuids = np.random.normal(1,1,nb_uuids)
			offset = abs(np.min(uuids))
			uuids = np.add(uuids, offset)
		elif rd == 'zipfian':
			uuids = np.random.zipf(self.zipf_distr_param, nb_uuids)
		else:
			print "Unsupported redundancy dstribution"
			exit()
		
		sizes = []
		if zd == 'random':
			sizes = np.random.randint(1,int(zf), nb_uuids)
		elif zd == 'gaussian':
			sizes = np.random.normal(1,1,nb_uuids)
			offset = abs(np.min(sizes))
			sizes = np.add(sizes, offset+1)
			sizes = np.remainder(sizes,int(zf))
		elif zd == 'zipfian':
			sizes = np.random.zipf(self.zipf_distr_param, nb_uuids)
			sizes = np.remainder(sizes,int(zf))
			sizes = np.add(sizes,1)
		else:
			print "Unsupported size dstribution"
			exit()
		
		deps_index = []
		if dd == 'random':
			deps_index = np.random.randint(0,nb_uuids, nb_uuids)
		elif dd == 'gaussian':
			deps_index = np.random.normal(1,1,nb_uuids)
			offset = abs(np.min(deps_index))
			deps_index = np.add(deps_index, offset)
		elif dd == 'zipfian':
			deps_index = np.random.zipf(self.zipf_distr_param, nb_uuids)
			deps_index = np.remainder(deps_index, nb_uuids)
		else:
			print "Unsupported dependency dstribution"
			exit()
		
		assigned = {}
		for i in range(0, int(sf)*l_imap):
			line = {}
			#print i
			line['loc'] = self.create_random_point(self.imap[int(loc_indexes[i])])

			line['time'] = int(timestamps[i])
			
			line['uid'] = int(uuids[i%nb_uuids])
			if uuids[i % nb_uuids] in assigned.keys():
				line['size'] = assigned[uuids[i % nb_uuids]][0]
				line['dep'] = assigned[uuids[i % nb_uuids]][1]
			else:
				line['size'] = int(sizes[i % nb_uuids])
				line['dep'] = uuids[int(deps_index[i % nb_uuids])]
				assigned[uuids[i % nb_uuids]] = (int(sizes[i % nb_uuids]), uuids[int(deps_index[i % nb_uuids])])
			amap.append(line)

		for line in amap:
			if line['dep'] not in uuids:
				print "You messed up something - dependency is not another request"

		self.dump_amap(amap, sd,sf,td,tf,zd,zf,dd,df,rd,rf)		

		# add uids & size
		#self.ticking()
		return amap

	def dump_amap(self, amap, sd,sf,td,tf,zd,zf,dd,df,rd,rf):
		dump_name = 'amaps/amap_' + sd[0]+sf+td[0]+tf+zd[0]+zf[0]+dd[0]+df+rd[0]+rf
		with open(dump_name, 'wt') as amap_dump:
			json.dump(amap,amap_dump)
		#print "written amap in ", dump_name


