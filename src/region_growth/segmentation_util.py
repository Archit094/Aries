#A simple util to make region growth work for dragonfly topology
import os
from region_growth import *

def get_link_to_region(link_to_stall,th1,th2,sz):
    n = len(link_to_stall)
    link_to_id = {}
    id_to_link = {}
    rtr_to_links = {}
    idx = 0
    for link in link_to_stall:
        link_to_id[link] = idx
        id_to_link[idx] = link
        idx +=1

    g = [[] for _ in range(0,n)]


    for link in link_to_stall:
        rtr_to_links[link[0][0]] = []
        rtr_to_links[link[1][0]] = []

    for link in link_to_stall:
        rtr1 = link[0][0]
        rtr2 = link[1][0]
        rtr_to_links[rtr1].append(link)
        rtr_to_links[rtr2].append(link)

    for rtr in rtr_to_links:
        links = rtr_to_links[rtr]
        for i in range(0,len(links)):
            for j in range(i+1,len(links)):
                id1 = link_to_id[links[i]]
                id2 = link_to_id[links[j]]
                g[id1].append(id2)
                g[id2].append(id1)

    vals = [0 for _ in range(0,n)]
    for link in link_to_stall:
        vals[link_to_id[link]] = link_to_stall[link]


    ##for lis in g:
      #  if(len(lis)>0):
       #     print(lis)
    seg = Segmentation(g,vals)
    region_nums = seg.form(th1)
    region_nums = seg.merge1(region_nums,th2)
    region_nums = seg.merge2(region_nums,sz)
    
    link_to_region = {}
    for i in range(0,n):
        link_to_region[id_to_link[i]] = region_nums[i]
    return link_to_region


'''link_to_stall = { (('a',0),('z',2)):10 , (('a',1),('y',4)):2 , (('a',2),('x',4)):10, (('x',3),('w',4)) : 2}

print(get_link_to_region(link_to_stall))'''
