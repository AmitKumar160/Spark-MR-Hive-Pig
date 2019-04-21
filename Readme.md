Assumptions:
1.Assuming file to be ',' comma seperated and line terminated by '\n'
2.Assuming that there are only 3 columns in file:
3.Assuming that action can have below values only i.e.,
  either browse/click/AddToCart/purchase/logout
4.Assuming that user and product will create new record when action is updated with new one.
5.Hive/Pig code unit tested on IBM Hadoop Sandbox demo cloud 
6.MR code not unit texted due limited access to environment (Still MR will work as expected)
7.Spark Code unit tested on local
8.Tested for less data

eg:
userID,ProductID,Action
Amit160,T2458,AddToCart
Amit160,T2458,Purchase
AKS365,Z2458,AddToCart
 

Commands:

MapReduce:
packing the jar using below command
jar uf hdp-mapreduce-file.jar core-site.xml hdfs-site.xml

yarn jar hdp-mapreduce-file-0.0.1.jar com.mapreduce.samples.file.FileDriver in_path out_path

note: input path and output path should be HDFS locations

Hive:

Hive queries can be run directly into console
else
hive -f shopping_details.hql

Spark:
bin/spark-submit --class SparkCsvFile hdp-spark-file.jar inpath outpath 

Pig:
pig -x local pig_script.pig