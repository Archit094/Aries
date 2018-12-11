def get_rtr_port(st):
#     print(st)
    rtr  = st.split('a')[0]
    port_f = st.split('l')[1]
    port_f = port_f.split('(')[0]
    port = int(port_f[0])*8+int(port_f[1])
    return (rtr,port)

def load_edges_from_file(file_path = '../OVIS_DATA/rtr.out'):
    #rtr_port to adjacent rtr_port
    edges = {}
    #edge to color
    edgec  ={}
    f = open(file_path)
    for line in f:
        cols = line.split()
        if(cols[3]=='unused' or cols[3]=='processor'):
            continue
        edges[get_rtr_port(cols[0])] = get_rtr_port(cols[3])
        edgec[get_rtr_port(cols[0])] = cols[1]
    f.close()
#     print(edges)
    return (edges,edgec)