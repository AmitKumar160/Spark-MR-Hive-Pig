/*
Author: Amit Kumar
Date:13/01/2018
*/
A= LOAD 'file.csv' USING PigStorage(',') AS (userID:chararray,productID:chararray,action:chararray);
B= FILTER A BY action == 'AddToCart';
C= FILTER A BY action == 'Purchase';
D= FILTER B BY NOT userID IN (C.userID);
E= FOREACH D GENERATE userID,productID;
Dump E;