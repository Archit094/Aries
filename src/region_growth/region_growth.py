from UF import *
class Segmentation:
    def __init__(self,g,vals):
        self.g = g
        self.vals = vals

    def form(self,threshold):
        vals = self.vals
        g = self.g
        n = len(vals)
        ordered_v = [v for _,v in sorted(zip(vals,[i for i in range(0,n)]))]
        visited = [0 for _ in range(0,n)]
        seeds = []
        regions = []
        seeds.append(ordered_v[0])
        seed_ptr = 0
        for u in range(0,n):
            if(visited[u]==1):
                continue

            current_region = set([u])
            current_seeds = [u]
            visited[u] = 1
            ptr = 0
            while(ptr<len(current_seeds)):
                u = current_seeds[ptr]
                ptr +=1
                for v in g[u]:
                    if(visited[v]==0 and abs(vals[u]-vals[v])<threshold):
                        current_region.add(v)
                        visited[v] = 1
                        current_seeds.append(v)
            regions.append(list(current_region))

        region_nums  = [-1 for _ in range(0,n)]
        rnum = -1
        for region in regions:
            rnum+=1
            for u in region:
                region_nums[u] = rnum

        return region_nums
    
    def merge1(self,region_nums,threshold):
        vals = self.vals
        g = self.g
        n = len(vals)
        region_to_total = {}
        region_to_count = {}
        uf = UF(n)

        for u in range(0,n):
            region_to_total[region_nums[u]] = 0
            region_to_count[region_nums[u]] = 0

        for u in range(0,n):
            region_to_total[region_nums[u]] += vals[u]
            region_to_count[region_nums[u]] +=1

        for u in range(0,n):
            for v in g[u]:
                region1 = region_nums[u]
                region2 = region_nums[v]
                avg1 = region_to_total[region1]/region_to_count[region1]
                avg2 = region_to_total[region2]/region_to_count[region2]
                if(abs(avg1-avg2)<threshold):
                    uf.union(region1,region2)

        region_nums_new = [-1 for i in range(0,n)]
        for u in range(0,n):
            region_nums_new[u] = uf.find(region_nums[u])

        return region_nums_new 

    def merge2(self,region_nums,threshold):
        vals = self.vals
        g = self.g
        n = len(vals)
        region_to_size  = {}
        uf = UF(n)
        for u in range(0,n):
            region_to_size[region_nums[u]] = 0
        for u in range(0,n):
            region_to_size[region_nums[u]] += 1

        visited = set()
        for u in range(0,n):
            for v in g[u]:
                region1 = region_nums[u]
                region2 = region_nums[v]
                size1 = region_to_size[region1]
                size2 = region_to_size[region2]
                if((size1<threshold and size2>=threshold) and region1 not in visited):
                    uf.union(region1,region2)
                    visited.add(region1)
                if((size1>=threshold and size2<threshold) and region2 not in visited):
                    uf.union(region1,region2)
                    visited.add(region2)

        region_nums_new = [-1 for _ in range(0,n)]
        for u in range(0,n):
            region_nums_new[u] = uf.find(region_nums[u])
        return region_nums_new


'''g = [[1,2],[0,2,3],[0,1],[2,4],[3]]
vals = [10,2,10,2,100]
seg = Segmentation(g,vals)
region_nums = seg.form(2)
region_nums = seg.merge1(region_nums,20)
region_nums = seg.merge2(region_nums,2)
print(region_nums)
'''


