import os
# os.system('module load python/3.6-anaconda-5.2')
import pandas as pd
import csv
from segmentation_util import *
def write_csv(data,file_path):
    with open(file_path,'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)
    csvFile.close()

def generate(input_dir,output_dir,p1,p2,p3,param,start,end):
    def parse_df(start,end):
        df = pd.read_csv(input_dir+'rtr_'+str(start)+'_'+str(end))
        header = ['time','rtr1','port','flit','stall','spf_porte','rtr2','port2','color']
        df.columns = header
        df = df.drop_duplicates()
        return df

    df = parse_df(start,end)
    region_to_links = {}
    region_to_time = {}
    region_to_total_stall = {}
    region_to_time_final = {}
    region_to_timestamp = {}
    region_to_region = {}
    region = 0
    cnt = 0
    for time in range(end,start,-1):
        cnt +=1
        if(cnt%20==0):
            print('Done with processing %d time points'%(cnt))
        #if(cnt==100):
        #    break
        dft = df[(df.time==time)]
        nodes = set()
        edges_g = []
        router_to_links = {}
        link_to_stall = {}
        for index,row in dft.iterrows():
            rtr = row['rtr1']
            port = row['port']
            spf = row[param]
            clr = row['color']
            rtr2 = row['rtr2']
            port2 = row['port2']
            link_to_stall[((rtr,port),(rtr2,port2))] = spf
            
        if(len(link_to_stall)==0):
            continue
        link_to_region = get_link_to_region(link_to_stall,p1,p2,p3)

        region_to_links_temp = {}
        for link in link_to_region:
            region_to_links_temp[link_to_region[link]] = []
        
        
        for link in link_to_region:
            region_to_links_temp[link_to_region[link]].append(link)


        curr_regions = set()
        for _,comp in region_to_links_temp.items():
            region += 1
            curr_regions.add(region)
            region_to_links[region] = []
            region_to_total_stall[region] = 0
            for link in comp:
                region_to_links[region].append(link)
                region_to_total_stall[region] += link_to_stall[link]
                
        region_to_time_temp = {}
        
        get_score = {}
        for region1 in curr_regions:
            for region2 in region_to_time:
                get_score[(region1,region2)] = float(len(set(region_to_links[region1]).intersection(set(region_to_links[region2]))))/min(len(region_to_links[region1]),len(region_to_links[region2]))
                
        for region1 in curr_regions:
            flag = 0
            region_to_timestamp[region1] = time
            for region2 in region_to_time:
                as1 = region_to_total_stall[region1]/len(region_to_links[region1])
                as2 = region_to_total_stall[region2]/len(region_to_links[region2])
                if(get_score[(region1,region2)]>=0.5 and abs(as1-as2)<=2e7):
                    region_to_region[region1] = region2 
                    region_to_time_temp[region1] = region_to_time[region2]+1 #((rtr,port),(rtr2,port2))
                    flag = 1
                    break
            if(flag==0):
                region_to_time_temp[region1] = 1
                
        region_to_time = region_to_time_temp
        region_to_time_final.update(region_to_time)
        
    # print(region_to_time_final)
    # print(region_to_region)
    data = [['Timestamp','Region1','Time1','SPF1','Size1','Region2','Time2','SPF2','Size2']]
    for region in region_to_time_final:
        if region in region_to_region:
            region2 = region_to_region[region]
            data.append([region_to_timestamp[region],region,region_to_time_final[region],region_to_total_stall[region],len(region_to_links[region]),region2,region_to_time_final[region2],region_to_total_stall[region2],len(region_to_links[region2])])
        else:
            data.append([region_to_timestamp[region],region,region_to_time_final[region],region_to_total_stall[region],len(region_to_links[region]),0,0,0,0])
    write_csv(data,output_dir+str(start)+'_'+str(end))

#generate(1497514451,1497514864)
