

1.Upload the 3 jar files into VM
2.Create a input folder using:
hdfs dfs -mkdir input
3.Upload user.dat into input :
hdfs dfs -put user.dat input

The steps to run Question1.jar
1. check if /output  already exists, if so, delete it first:
hdfs dfs -rm -r -f output
2. run jar file:
hadoop jar Question1.jar Question1 input output
3. show the output of the hadoop
hdfs dfs -cat output/*

The steps to run Quesion2.jar
1. check if /output  already exists, if so, delete it first:
hdfs dfs -rm -r -f output
2. run jar file:
hadoop jar Question2.jar Question2 input output
3. show the output of the hadoop
hdfs dfs -cat output/*

The steps to run Quesion3.jar
1. delete the user.dat in the input using 
hdfs dfs -rm -r -f input/user.dat
2. upload the movies.dat into the input:
hdfs dfs -put movies.dat input
3. check if /output already exists, if so, delete it first:
hdfs dfs -rm -r -f output
4. run jar file:
hadoop jar Question3.jar Question3 input output
5. show the output of the hadoop
hdfs dfs -cat output/*
