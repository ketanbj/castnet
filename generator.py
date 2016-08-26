#!/bin/python

import numpy as np
import sys

import matplotlib.pyplot as plt
#import scipy.special as sps

class Generator:

	def __init__(self, imap):
		self.imap = imap

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

	def show(self,s, dist):
		if dist == 'random':
			plt.plot(range(0, len(s), s))
			plt.show()
		elif dist == 'gaussian':
			mu = 1
			sigma = 1
			count, bins, ignored = plt.hist(s,100)
			plt.plot(bins, 1/(sigma * np.sqrt(2 * np.pi)) * np.exp( - (bins - mu)**2 / (2 * sigma**2) ),linewidth=2, color='r')
			plt.show()
		elif dist == 'zipfian':
			print "Some issue with scipy import"
			return
			#a =2
			#count, bins, ignored = plt.hist(s[s<50], 50)
			#x = np.arange(1., 50.)
			#y = x**(-a)/sps.zetac(a)
			#plt.plot(x, y/max(y), linewidth=2, color='r')
			#plt.show()

	def generate_amap(self, sd,sf,td,tf,zd,zf,dd,df,rd,rf):

		l_imap = len(self.imap)
		print "Number of i locations: ", l_imap
		amap = []		
		nb_uuids = (int(sf)*l_imap)/int(rf)
		
		loc_indexes = []
		if sd == 'random':
			loc_indexes = np.random.randint(0,l_imap, int(sf) * l_imap)
		elif sd == 'gaussian':
			loc_indexes = np.random.normal(1,1,int(sf)*l_imap)
			offset = abs(np.min(loc_indexes))
			s_indexes = np.add(loc_indexes, offset)
		elif sd == 'zipfian':
			loc_indexes = np.random.zipf(2,int(sf)*l_imap)
			loc_indexes = np.remainder(loc_indexes,l_imap-1)
			#print loc_indexes
		else:
			print "Unsupported spatial distribution"
			exit()

		self.show(loc_indexes,sd)

		timestamps = []
		if td == 'random':
			timstamps = np.random.randint(0, 86400, int(sf)*l_imap)
		elif td == 'gaussian':
			timestamps = np.random.normal(1, 1, int(sf) * l_imap)
			offset = abs(np.min(timestamps))
			timestamps = np.add(timestamps, offset)
			timestamps = np.remainder(timestamps, 86400)
		elif td == 'zipfian':
			timestamps = np.random.zipf(2, int(sf) * l_imap)
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
			uuids = np.random.zipf(2, nb_uuids)
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
			sizes = np.remainer(sizes,int(zf))
		elif zd == 'zipfian':
			sizes = np.random.zipf(2, nb_uuids)
			sizes = np.remainer(sizes,int(zf))
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
			deps_index = np.random.zipf(2, nb_uuids)
			deps_index = np.remainder(deps_index, nb_uuids)
		else:
			print "Unsupported dependency dstribution"
			exit()
		
		assigned = {}
		for i in range(0, int(sf)*l_imap):
			line = {}

			line['loc'] = self.imap[int(loc_indexes[i])]

			line['time'] = int(timestamps[i])
			
			line['uid'] = int(uuids[i])
			if uuids[i] in assigned.keys():
				line['size'] = assigned[uuids[i]][0]
				line['dep'] = assigned[uuids[i]][1]
			else:
				line['size'] = int(sizes[i % nb_uuids])
				line['dep'] = uuids[int(deps_index[i % nb_uuids])]
				assigned[uuids[i]] = (int(sizes[i % nb_uuids]), uuids[int(deps_index[i % nb_uuids])])
			amap.append(line)

		for line in amap:
			if line['dep'] not in uuids:
				print "You messed up something - dependency is not another request"
		
		# add uids & size
		self.ticking()
		return amap

