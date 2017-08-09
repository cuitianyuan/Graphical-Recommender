# Overview
Graphical recommender system using personalized PageRank (Random Walk with Restart)


## Background

#### Bipartite Graph Structure

A bipartite graph is a graph where nodes can be divided into two groups V1 and V2 such that no edge connects the vertices in the same group.

![Image](images/bipartite-graph.png?raw=true "Image")

#### Neighborhood formation: Random Walk with restart algorithm (personalized pagerank)
Given a query node a in V1, Neighborhood Formation computes the relevance scores of all the nodes in V1 to a. 

Random walk with restart is the 'personalized' rangerank algorithm. It models the distribution of  rank, given that the distance random walkers can travel from their source is determined by alpha. For each iteration, the probability of going to a connected node is (1-alpha)/(#of connected node), and there is also a alpha probability for the walker to go to a random node. You can refer to the [literature](literature/) folder for more details of the algorithm.  

In this dataset, given a patient(user) node, the RWR algorithm will compute and output the closest 10 neighboring user. 

![Image](images/Neighborhood-formation.png?raw=true "Image")

In the simulated data above, user2 and user3 are connected by Med2&3 and Lab2&3, whereas user1 doesn't have any shared node connected with user2/3's neighborhood. Therefore, when running the [proof of concept python code](proof_of_concept_pagerankRWR.py) you will see that the closest neighbor (user edge) for user2 (beside himself) is user3. 

![Image](images/simulation_result.png?raw=true "Image")


## Dependencies
* Java 1.8+
* Scala 2.1+
* built.sbt



## References
1. [Epasto et al. Reduce and Aggregate: Similarity Ranking in Multi-Categorical Bipartite Graphs](http://www.epasto.org/papers/reduce-aggregate.pdf)
2. [Sun et al. Neighborhood Formation and Anomaly Detection in Bipartite Graphs](http://www.cs.cmu.edu/~deepay/mywww/papers/icdm05.pdf)
3. [GraphX personalized pagerank](https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala)

## To do
1. Other kinds of similarity matrix
	* Search_Query - Search_Query
	* Onet - Onet
	* User - User (will incvolve overnight update)
2. Expand number of nodes. The experiments in A Epasto et al used billions of node.
3. Create the parallel prediction method. 
4. Expand number of companies after creating the parallel prediction method. 
