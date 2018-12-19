#Read comments for corresponding link file
import pandas as pd
import os
import sys
import re
import csv
from graph_util import *
def create_csvs(data_dir,fpx,base_time,start,end):
    # data_dir = '../OVIS_DATA/LDMS_CSV/'
    def output_rtr_df_header(file_path,file_header_path,output_name,time_stamp,start_time,end_time):
        print(file_path)
        print(file_header_path)
        time_stamp = int(time_stamp)
        f = open(file_header_path)
        headers = []
        for line in f:
            headers = line.split(',')
        
        print('Started reading CSV into dataframe')
        df = pd.read_csv(file_path,names=headers)
        print('Read CSV into dataframe')
        
        flit_rgx = 'AR_NIC_RSPMON_PARB_EVENT_CNTR_PI_PKTS'
        stall_rgx = 'AR_NIC_RSPMON_PARB_EVENT_CNTR_IOMMU_PKTS'
        
        useful_headers = ['#Time','component_id',flit_rgx,stall_rgx]
        #print(df.columns)
        df = df[useful_headers]
        df =df.rename(index=str, columns={'#Time': 'time',flit_rgx:'flit',stall_rgx:'stall'})
        df.time = df.time.astype(int)
        df = df[(df.time>=start_time) & (df.time<=end_time)]
        df = df.sort_values(by=['time','component_id'])
        NUM_ROUTER = len(set(df['component_id']))
        cache_stall = {}
        cache_flit = {}
        cnt = 0
        cnt2 = 0
        with open(fpx,'a') as fx:
            writer = csv.writer(fx, delimiter=',')
            print(NUM_ROUTER)
            #### HACKY FIX STARTS ###
            time_stamp = start_time  
            #### HACKY FIX ENDS #####
            
            for index,row in df.iterrows():
                cnt2 +=1
                if(cnt2%10000==0):
                    print('Done with %d rows'%(cnt2))
                    
                rtr = row['component_id']
                time = int(row['time'])-int(time_stamp)
                i = 0
                tot_stall = row['stall']
                tot_flit =  row['flit']
                
                MAX_SIZE = 2**64-1
                if(rtr in cache_stall and rtr in cache_flit):
                    flit_delta = tot_flit-cache_flit[rtr]
                    if(flit_delta<0):
                        flit_delta = MAX_SIZE+flit_delta
                    stall_delta = tot_stall-cache_stall[rtr]
                    if(stall_delta<0):
                        stall_delta = MAX_SIZE+stall_delta
    

                    row = [int(time+time_stamp),rtr,flit_delta,stall_delta]

                    writer.writerow(row)

                cache_flit[rtr] = tot_flit
                cache_stall[rtr] = tot_stall



    def output_rtr_df(time_stamp,start_time,end_time):
        file_paths = []
        file_path_headers = []
        output_names = []
        base_names = ['metric_set_nic.']
        for base in base_names:
            file_paths.append(data_dir+base+str(time_stamp))
            file_path_headers.append(data_dir+base+'HEADER.'+str(time_stamp))
            output_names.append(base+str(time_stamp))
            
        output_rtr_df_header(file_paths[0],file_path_headers[0],output_names[0],time_stamp,start_time,end_time)
    
    return output_rtr_df(base_time,start,end)

#create_csvs('../nic_processed/trial',1497648110,1497648348,1497648519)
