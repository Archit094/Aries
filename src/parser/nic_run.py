#!/usr/bin/python3
import os
import nic_processor_proc
import nic_processor_hsn
import nic_processor_iommu
import pandas as pd
from multiprocessing import Pool
import glog as log
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--ovis',help='Directory containing OVIS raw logs')
parser.add_argument('--output',help='Path to processed output')
parser.add_argument('--app',help='Path to app runs')
parser.add_argument('--thread',help='Number of threads')

args = parser.parse_args()
print(args)


ovis_dir = args.ovis
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
                if(f.startswith('metric_set_nic.HEADER.')):
                        base = int(f.split('.')[2])
                        if(check_intersect(start,end,base,base+3599)):
                                times.append((base,max(start,base),min(end,base+3599)))
        return times

def run_interval(times):
        start = times[0]
        end = times[1]
        info = get_base(start,end)
        print(info)
        nic_typ = ['proc','hsn','iommu']
        for typ in nic_typ:
            file_path = output_dir+'/nic_'+str(start)+'_'+str(end)+'_'+typ
            log.info('Started processing %d %d'%(start,end))
            try:
                for (base,startx,endx) in info:
                        print(base,startx,endx)
                        if(typ=='proc'):
                            nic_processor_proc.create_csvs(ovis_dir,file_path,base,startx,endx)
                        if(typ=='hsn'):
                            nic_processor_hsn.create_csvs(ovis_dir,file_path,base,startx,endx)
                        if(typ=='iommu'):
                            nic_processor_iommu.create_csvs(ovis_dir,file_path,base,startx,endx)
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
        #print(st)
        #print(en)
        # exists = os.path.isfile('../nic_processed_proc/nic_'+str(st)+'_'+str(en)+'.success')
        # run_interval((st,en))
        exists = False
        if not exists:
            args.append((st,en))
    except:
        pass
print(args)
p = Pool(num_threads)
p.map(run_interval, args)
log.info('Done')

