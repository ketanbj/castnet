#!/bin/python

import numpy as np
import sys

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

	def generate_amap(self, sd,sf,td,tf,zd,zf,dd,df,rd,rf):
		l_imap = len(self.imap)
		print "Number of i locations: ", l_imap
		amap = []
		
		nb_uuids = (int(sf)*l_imap)/int(rf)
		uuids = range(0,nb_uuids)
		
		if sd == 'gaussian':
			gs_indexes = np.random.normal(1,1,int(sf)*l_imap)
			offset = abs(np.min(gs_indexes))
			gs_indexes = np.add(gs_indexes, offset)
			print len(gs_indexes)

		if td == 'gaussian':
			gt_timestamps = np.random.normal(1, 1, 86400)
			offset = abs(np.min(gt_timestamps))
			gt_indexes = np.add(gt_timestamps, offset)

		for i in range(0, int(sf)*l_imap):
			line = {}

			if sd == 'random':
				index = np.random.randint(0,l_imap)
				line['loc'] = self.imap[index]
			elif sd == 'gaussian':
				index = int(gs_indexes[i])
				line['loc'] = self.imap[index]
			else:
				print "Unsupported spatial distribution"
				exit()

			if td == 'random':
				line['time'] = np.random.randint(0, 86400)
			elif td == 'gaussian':
				line['time'] = int(gt_timestamps[i%86400])
			else:
				print "Unsupported temporal distribution"	

			line['uid'] = -1
			amap.append(line)


		for line in amap:
			if rd == 'random':
				if line['uid'] == -1:
					new_uid = np.random.randint(0,nb_uuids)
					#check if we already have a line for this uid
					check_size = 0
					check_dep = -1
					for check_line in amap:
						if check_line['uid'] == new_uid:
							check_size = check_line['size']
							check_dep = check_line['dep']
							break;

					if check_size is not 0:
						line['size'] = check_size
					else:
						if zd == 'random':
							line['size'] = np.random.randint(0,int(zf))
					
					if check_dep is not -1:
						line['dep'] = check_dep
					else:
						if dd == 'random':
							line['dep'] = np.random.randint(0,int(df)*l_imap)
					line['uid'] = new_uid
				self.ticking()
		return amap

