import pandas as pd
import os
import sys
import re
import csv
from graph_util import *

#Creates processed CSV from raw data
def create_csvs(fpx,data_dir,rtr_file,base_time,start,end):
    # data_dir = '../OVIS_DATA/LDMS_CSV/'
    def output_rtr_df_header(file_path,file_header_path,output_name,time_stamp,start_time,end_time):
        time_stamp = int(time_stamp)
        f = open(file_header_path)
        headers = []
        for line in f:
            headers = line.split(',')
        
        print('Started reading CSV into dataframe')
        df = pd.read_csv(file_path,names=headers)
        print('Read CSV into dataframe')
        
        #Regex for flit and stall columns
        flit_rgx = 'AR_RTR_(\d_\d)_INQ_PRF_INCOMING_FLIT_VC(\d)'
        stall_rgx = 'AR_RTR_(\d_\d)_INQ_PRF_ROWBUS_STALL_CNT'
        
        #Only care about column names with these regex
        useful_headers = ['#Time','aries_rtr_id',flit_rgx,stall_rgx]
        drop_cols = []
        for col in df.columns:
            chk = 0
            for u_header in useful_headers:
                if(re.match(u_header,col)):
                    chk = 1
            if(chk==0):
                drop_cols.append(col)
                
        #Drop all other columns
        df = df.drop(drop_cols,axis=1)
        # print(df.columns)
        df =df.rename(index=str, columns={'#Time': 'time'})
        df.time = df.time.astype(int)
        #Remove time points from dataframe not lying in the required range
        df = df[(df.time>=start_time) & (df.time<=end_time)]
        types = []
        ports = []
        for col in df.columns:
            if(re.match(flit_rgx,col)):
                clis = col.split('_')
                port = int(clis[2])*8 + int(clis[3])
                types.append(0)
                ports.append(port)
            elif(re.match(stall_rgx,col)):
                clis = col.split('_')
                port = int(clis[2])*8 + int(clis[3])
                types.append(1)
                ports.append(port)
            else:
                types.append(-1)
                ports.append(-1)
        df = df.sort_values(by=['time','aries_rtr_id'])
        NUM_ROUTER = len(set(df['aries_rtr_id']))
        NUM_PORTS  = 40
        VC_COUNT = 8
        #Store previous encountered values for stall and flit
        cache_stall = [[ 0 for j in range(0,NUM_PORTS)] for i in range(0,NUM_ROUTER)]
        cache_flit = [[0 for j in range(0,NUM_PORTS)] for i in range(0,NUM_ROUTER)]
        
        
        idx = 0
        id_to_rname = ['' for i in range(0,NUM_ROUTER)]
        rname_to_id = {}
        rnames = list(set(df['aries_rtr_id']))
        rnames.sort()
        for rname in rnames:
            id_to_rname[idx] = rname
            rname_to_id[rname] = idx
            idx +=1
        
        cnt = 0
        cnt2 = 0
        #Iterate over all rows in raw CSV and add rows to the processed CSV
        (edges,edgec) = load_edges_from_file(rtr_file)
        with open(fpx,'a') as fx:
            writer = csv.writer(fx, delimiter=',')
            print(NUM_ROUTER)
            #### HACKY FIX STARTS ###
            time_stamp = start_time  
            #### HACKY FIX ENDS #####
            
            for index,row in df.iterrows():
                cnt2 +=1
                if(cnt%10000==0):
                    print('Done with %d rows'%(cnt))
                    
                rid = rname_to_id[row['aries_rtr_id']]
                time = int(row['time'])-int(time_stamp)
                i = 0
                tot_stall = [-1 for j in range(0,NUM_PORTS)]
                tot_flit =  [-1 for j in range(0,NUM_PORTS)]
                for col in df.columns:
                    if(types[i]==1):
                        port = ports[i]
                        val = row[col]
                        tot_stall[port] += val
                    if(types[i]==0):
                        port = ports[i]
                        val = row[col]
                        tot_flit[port] += val
                    i+=1
                
                MAX_SIZE = 2**64-1
                for port in range(0,NUM_PORTS):
                    if(tot_stall[port]!=-1 and tot_flit[port]!=-1):
                        flit_delta = tot_flit[port]-cache_flit[rid][port]+1
                        if(flit_delta<0):
                            flit_delta = MAX_SIZE+flit_delta
                        stall_delta = tot_stall[port]-cache_stall[rid][port]+1
                        if(stall_delta<0):
                            stall_delta = MAX_SIZE+stall_delta
                        rtr2,port2 = edges[(id_to_rname[rid],port)]
                        clr = edgec[(id_to_rname[rid],port)]
                        row = [int(time+time_stamp),id_to_rname[rid],port,flit_delta,stall_delta,float(stall_delta)/flit_delta,rtr2,port2,clr]
                        if(cache_flit[rid][port]!=0 and cache_stall[rid][port]!=0):
                            writer.writerow(row)
                        cache_flit[rid][port] = tot_flit[port]
                        cache_stall[rid][port] = tot_stall[port]
                cnt +=1
    def output_rtr_df(time_stamp,start_time,end_time):
        #All files which have the whole set of raw logs
        file_paths = []
        file_path_headers = []
        output_names = []
        base_names = ['metric_set_rtr_0_2','metric_set_rtr_1_2','metric_set_rtr_2_2','metric_set_rtr_3_2']
        for base in base_names:
            file_paths.append(data_dir+base+'_c.'+str(time_stamp))
            file_paths.append(data_dir+base+'_s.'+str(time_stamp))
            file_path_headers.append(data_dir+base+'_c.HEADER.'+str(time_stamp))
            file_path_headers.append(data_dir+base+'_s.HEADER.'+str(time_stamp))
            output_names.append(base+'_c.'+str(time_stamp))
            output_names.append(base+'_s.'+str(time_stamp))
            
        output_rtr_df_header(file_paths[0],file_path_headers[0],output_names[0],time_stamp,start_time,end_time)
        #print(len(data1))
        output_rtr_df_header(file_paths[4],file_path_headers[4],output_names[4],time_stamp,start_time,end_time)
        #print(len(data2))
    
    return output_rtr_df(base_time,start,end)

# create_csvs('../processed5/trial','../OVIS_DATA/LDMS_CSV/','../OVIS_DATA/rtr.out',1497648110,1497648348,1497648519)
