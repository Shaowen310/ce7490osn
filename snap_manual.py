# %%
# references:
# https://snap.stanford.edu/snappy/doc/reference/graphs.html#
# https://snap.stanford.edu/snappy/index.html#introduction
import snap

# %%
# create a graph PNGraph
G1 = snap.TNGraph.New()
G1.AddNode(1)
G1.AddNode(5)
G1.AddNode(32)
G1.AddEdge(1,5)
G1.AddEdge(5,1)
G1.AddEdge(5,32)

# %%
# iterators
# create a directed random graph on 100 nodes and 1k edges
G2 = snap.GenRndGnm(snap.PNGraph, 100, 1000)
# traverse the nodes
for NI in G2.Nodes():
    print("node id %d with out-degree %d and in-degree %d" % (
        NI.GetId(), NI.GetOutDeg(), NI.GetInDeg()))
# traverse the edges
for EI in G2.Edges():
    print("edge (%d, %d)" % (EI.GetSrcNId(), EI.GetDstNId()))
# traverse the edges by nodes
for NI in G2.Nodes():
    for Id in NI.GetOutEdges():
        print("edge (%d %d)" % (NI.GetId(), Id))

# %%
# I/O
# generate a network using Forest Fire model
G3 = snap.GenForestFire(1000, 0.35, 0.35)
# save and load binary
FOut = snap.TFOut("test.graph")
G3.Save(FOut)
FOut.Flush()
FIn = snap.TFIn("test.graph")
G4 = snap.TNGraph.Load(FIn)
# save and load from a text file
snap.SaveEdgeList(G4, "test.txt", "Save as tab-separated list of edges")
G5 = snap.LoadEdgeList(snap.PNGraph, "test.txt", 0, 1)

# %%
# Manipulate
# generate a network using Forest Fire model
G6 = snap.GenForestFire(1000, 0.35, 0.35)
# convert to undirected graph
G7 = snap.ConvertGraph(snap.PUNGraph,G6)
WccG = snap.GetMxWcc(G6)
# get a subgraph induced on nodes {0,1,2,3,4,5}
SubG = snap.GetSubGraph(G6, snap.TIntV.GetV(0,1,2,3,4))
# get 3-core of G
Core3 = snap.GetKCore(G6, 3)
# delete nodes of out degree 10 and in degree 5
snap.DelDegKNodes(G6, 10, 5)

# %%
# stats
# generate a Preferential Attachment graph on 1000 nodes and node out degree of 3
G8 = snap.GenPrefAttach(1000, 3)
# vector of pairs of integers (size, count)
CntV = snap.TIntPrV()
# get distribution of connected components (component size, count)
snap.GetWccSzCnt(G8, CntV)
# get degree distribution pairs (degree, count)
snap.GetOutDegCnt(G8, CntV)
# vector of floats
EigV = snap.TFltV()
# get first eigenvector of graph adjacency matrix
snap.GetEigVec(G8, EigV)
# get diameter of G8
snap.GetBfsFullDiam(G8, 100)
# count the number of triads in G8, get the clustering coefficient of G8
snap.GetTriads(G8)
snap.GetClustCf(G8)
# %%
