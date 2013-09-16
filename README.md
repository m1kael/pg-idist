pg-idist
========

A PL/pgSQL implementation of the iDistance algorithm for multi-dimensional
data indexing and retrieval. The algorithm is especially well-suited for
real-world high-dimensional point data and kNN queries.

This is a prototyped version featuring a single, multi-dimensional array column field for simplicity. An extensible version with user defineable table and columns will be released with a technical report in the near future. 

Updates will be available on github.
https://github.com/m1kael/pg-idist


Quick Instructions
--------
1.  Install and setup postgresql
2.  Create a psql user (for ease, match your linux username)
3.  Create database named 'idist' owned by your user account
4.  Log into the database
5.  Load the following scripts:
    \i tables.sql
    \i functions.sql
6.  Execute the following commands:
    select * from Test();


Basics
--------
This explains the basic concepts and the table layouts. 

Essentially we have a collection of points, where each point has an ID and an array of real values representing it's point in multi-dimensional space. We also have a set of reference points (which are defined the same way) used to create partitions in the dataspace much like a voronoi diagram. Each point is assigned to the closest reference point and an index value is calculated based on this distance, which is then stored in the index table (the val field) for each point (by ID). 

A KNN query requires a query point in the give multi-dimensional space, and a number (k) of nearest neighbors to retrieve. The algorithm starts with an initial radius size and iteratively increases until the k true nearest neighbors are found. For each iteration, all partitions are checked for overlap with the querysphere and searched where applicable. Due to the lossy mapping index function, this returns filtered candidates that must still be refined before being added to the true k-set. Importantly, it will always return the exact results, and the search can be terminated as soon as they are found. In other words, the algorithm can be certain when no other points exist in the dataset that could be closer than the given k-set already found.



References
--------

For now, please refer to our publication which first introduces our work and open source implementation of idistance. 

Schuh, Michael A., et al. "A Comprehensive Study of iDistance Partitioning Strategies for kNN Queries and High-Dimensional Data Indexing." Proc. of the 29th BNCOD Conf. 2013.

