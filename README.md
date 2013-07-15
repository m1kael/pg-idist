pg-idist
========

A PL/pgSQL implementation of the iDistance algorithm for multi-dimensional
data indexing and retrieval. The algorithm is especially well-suited for
real-world high-dimensional point data and kNN queries.

Quick Instructions
--------
1.  Install and setup postgresql
2.  Create a psql user (for ease, match your linux username)
3.  Create database named 'idist' owned by your user account
4.  Log into the database
5.  Load the following scripts:
    \i tables.sql
    \i functions.sql
6.  Execute the following command:
    select * from Test(); 


Basics
--------
Explain the concepts and the basic table layouts.


References
--------
Several papers, documents, code itself, etc.
