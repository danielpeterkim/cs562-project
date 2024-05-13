# CS562 Project : Complier for Extended ESQL Language

A simple complier based on "Evaluation of Ad Hoc OLAP : In-Place Computation" by Damianos Chatziantoniou.

Inputs for the code are in the format of 6 Phi operands as in the paper. This complier is in EMF format with EMF functionality.
The defining conditions of the grouping variables (the 5th operand) will be in individual lines, followed by the having clause.
(see example txt files for more details, where queries are made in SQL, ESQL, and another txt file that will be read by our program)

## Background:
The goal of this extended SQL Language is to remove the strict relationship between grouping variables and their aggregate functions. 
