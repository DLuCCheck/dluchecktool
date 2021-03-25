## Problem
A bank merger causes a bank to have to work with multiple tables in different
but similar formats. There's no easy trustworthy way of linking rows in this
tables.


## What we know
Both tables should have related fields which should contain the same, or similar
information.


## Solution
We could serialize the two tables into a single format by:
- specifying the types of the columns in the two related tables and the type we
    desire them both to be.
- Apply a function in the contents of each field to convert them to the format
    that we want.
- Specify a relationship metric between  rows, this can be done by defining how
    likely are two fields to be related, then by checking each field in the
    rows we can get the overall likelihood of two rows being related, from this
    we can create a result table which contains the most likely related rows
    values paired and their likelihood to be equal.

## Problems with the solution
It's expensive both in memory and computationally as it requires to iterate
through every row in two tables 

We need a way to convert from tab a to tab b and from tab b to tab a.

