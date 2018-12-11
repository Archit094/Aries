#!/usr/bin/python3
import os
from link_processor import *
import pandas as pd
from multiprocessing import Pool
import glog as log
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--ovis',help='Directory containing OVIS raw logs')
parser.add_argument('--rtr',help='Path to rtr.out file')
parser.add_argument('--output',help='Path to processed output')
parser.add_argument('--app',help='Path to app runs')
parser.add_argument('--thread',help='Number of threads')
args = parser.parse_args()
print(args)


ovis_dir = args.ovis
rtr_file = args.rtr
output_dir = args.output
runs_file = args.app
num_threads = int(args.thread)


def check_intersect(x1,x2,y1,y2):
        return x1<=y2 and y1<=x2

def write_csv(data,file_path):
    with open(file_path,'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)
    csvFile.close()

def get_base(start,end):
        times = []
        files  = os.listdir(ovis_dir)
        for f in files:
                if(f.startswith('metric_set_rtr_0_2_c.HEADER.')):
                        base = int(f.split('.')[2])
                        if(check_intersect(start,end,base,base+3599)):
                                times.append((base,max(start,base),min(end,base+3599)))
        return times

def run_interval(times):
        start = times[0]
        end = times[1]
        info = get_base(start,end)
        file_path = output_dir+'rtr_'+str(start)+'_'+str(end)
        log.info('Started processing %d %d'%(start,end))
        try:
                for (base,startx,endx) in info:
                        print(base,startx,endx)
                        create_csvs(file_path,ovis_dir,rtr_file,base,startx,endx)
                with open(file_path+'.success','w') as fx:
                        fx.write('success')
        except:
                log.info('Error with processing %d %d'%(start,end))
                return
        log.info('Completed processing %d %d'%(start,end))

#run_interval([1497510193,1497510425])
df = pd.read_csv(runs_file)
args = []
for index,row in df.iterrows():
    try:
        st = int(row['starttime'])
        en = int(row['endtime'])
        exists = os.path.isfile(output_dir+'rtr_'+str(st)+'_'+str(en)+'.success')
        if not exists:
                args.append((st,en))
    except:
        pass


p = Pool(num_threads)
p.map(run_interval, args)
log.info('Done')