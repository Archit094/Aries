#!/usr/bin/python3 
import os
import pandas as pd
from multiprocessing import Pool
import glog as log
import sys
from region_processor import *
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--processed',help='Path to processed link logs')
parser.add_argument('--output',help='Path to processed output')
parser.add_argument('--done',help='Path to directory having done files')
parser.add_argument('--app',help='Path to app runs')
parser.add_argument('--thread',help='Number of threads')
parser.add_argument('--p1',help='Link level threshold')
parser.add_argument('--p2',help='Region level threshold')
parser.add_argument('--p3',help='Minimum region size')
parser.add_argument('--param',help='PTS or SPF')
args = parser.parse_args()
print(args)
processed_dir = args.processed
output_dir = args.output
runs_file = args.app
done_dir = args.done
num_threads = int(args.thread)
p1 = int(args.p1)
p2 = int(args.p2)
p3 = int(args.p3)
param = args.param
if(param!='PTS' and param!='SPF'):
	print('Invalid parameter option !')
	exit()

if(param=='PTS'):
	p1 = p1*1e7
	p2 = p2*1e7
if(param=='PTS'):
	param = 'stall'
if(param=='SPF'):
	param = 'spf_porte'

def run_interval(times):
	start = times[0]
	end = times[1]
	log.info('Started running function for %d %d'%(start,end))
	# try:
	generate(processed_dir,output_dir,p1,p2,p3,param,int(start),int(end))
	log.info('Completed running function for %d %d'%(start,end))
	with open(done_dir + str(start) + "_" + str(end) + "_region_transition"+".success", "w") as f:
		f.write("done")
	# except:
	# 	log.info('Error in plotting %d %d'%(start,end))
		


df = pd.read_csv(runs_file)
args = []
print(args)
for index,row in df.iterrows():
	try:
		st = int(row['starttime'])
		en = int(row['endtime'])
		exists = False
		# if not exists:
		args.append((st,en))
	except:
		pass

p = Pool(num_threads)
p.map(run_interval, args)
log.info('Done')	
