# -*- coding: utf-8 -*-
from operator import add
from pyspark import SparkContext

def readGraphRDD(input_file,SparkContext):
    """ read graph as RDD

    """
    return SparkContext.textFile(input_file).map(eval)

def swap(graphRDD):
    """swap graph

    """
    return graphRDD.map(lambda (u,v):(v,u))

def getDegree(graphRDD):
    """ get the degree of graph
    """
    d1=graphRDD.map(lambda (u,v):(u,1)).reduceByKey(add)
    d2=swap(graphRDD).map(lambda (u,v):(u,1)).reduceByKey(add)
    d=d1.union(d2)
    degrees=d.reduceByKey(add)
    return degrees

def getWLColor(graphRDD):
    """get the WL coloring
    """
    d=graphRDD.union(swap(graphRDD)).partitionBy(2).cache()  #union graph
    nodes=d.keys().distinct()   #nodes
    color=nodes.map(lambda x:(x,1))   #color
    num=1
    k=0
    while k>=0:
        joined=d.join(color)   #join union graph with color
        ncolor=joined.map(lambda (x,(n,c)):(n,(x,c))\
                     .groupByKey()\
                     .map(lambda (x,y):(x,[z[1] for z in y])) #get nodes and its neighborhoods' color
        color=ncolor.map(lambda (node,lst):(node,hash(str(sorted(lst)))))   #node and its color 
        k+=1
        new_num=swap(color).reduceByKey(add).map(lambda x:1).reduce(add)   #number of unique colors
        if num==new_num:
            break
        else:
            num=new_num
    return color


def degreeDis(degree):
    return degree.map(lambda (x,y):(y,1)).reduceByKey(add)

def isomorphic(graphRDD1,graphRDD2):
    #firstly compute two color distribution
    color1=getWLColor(graphRDD1).partitionBy(2).cache()
    color2=getWLColor(graphRDD2).partitionBy(2).cache()
    degree1=getDegree(graphRDD1).partitionBy(2).cache()
    degree2=getDegree(graphRDD2).partitionBy(2).cache()
    degree_dis1=degreeDis(degree1).count()
    degree_dis2=degreeDis(degree2).count()
    degree_dis=degreeDis(degree1).join(getDegree(graphRDD2))\
                                 .filter(lambda (x,(y,z)):y==z).count()
    #swap and groupByKey
    colorGroup1=swap(color1).groupByKey().mapValues(list)
    colorGroup2=swap(color2).groupByKey().mapValues(list)
    #decide if the same colors are present in both graphs
    if degree_dis1!=degree_dis2 or degree_dis!=degree_dis1:
        return "not_isomorphic"
    #decide if the number of colors is exactly n
    elif colorGroup1.count()!=color1.count() or colorGroup2.count()!=color2.count():
        return "maybe_isomorphic"
    else: 
        return "isomorphic"

          
         
        
if __name__ == '__main__':
    #question1
    sc=SparkContext() 
    graphRDD=sc.parallelize([(1,2),(1,4),(2,3),(2,4),(2,5),(3,5),(4,5)])
    degrees=getDegree(graphRDD)
    degrees.saveAsTextFile("degrees")
    #question2
    color=getWLColor(graphRDD)
    color.saveAsTextFile("color")
    #question3
    graphRDD1=sc.parallelize([(1,2),(1,4),(2,3),(2,4),(2,5),(3,5),(4,5)])
    graphRDD2=sc.parallelize([(1,3),(1,4),(1,5),(2,3),(2,5),(3,5),(4,5)])
    decide=isomorphic(graphRDD1,graphRDD2)
    f=open("decide","w")
    f.write("%s\n" % decide)
    f.close()
    if decide=="maybe_isomorphic" or decide =="isomorphic":
        color1=getWLColor(graphRDD1)
        color2=getWLColor(graphRDD2)
        colorGroup1=swap(color1).groupByKey().mapValues(list)
        colorGroup2=swap(color2).groupByKey().mapValues(list)
        joinedColor=colorGroup1.join(colorGroup2)
        joinedColor.values().saveAsTextFile("matching")
    #question4
    graphRDD_1=readGraphRDD("graph1.txt",sc)
    graphRDD_2=readGraphRDD("graph1.txt",sc)
    decide4=isomorphic(graphRDD_1,graphRDD_2)
    f=open("decide4","w")
    f.write("%s\n" % decide4)
    f.close()
    if decide4=="maybe_isomorphic" or decide4=="isomorphic":
        color_1=getWLColor(graphRDD_1)
        color_2=getWLColor(graphRDD_2)
        colorGroup_1=swap(color_1).groupByKey().mapValues(list)
        colorGroup_2=swap(color_2).groupByKey().mapValues(list)
        joined_Color=colorGroup_1.join(colorGroup_2)
        joined_Color.values().saveAsTextFile("matching4")


	

