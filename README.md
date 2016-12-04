# Hadoop-Project



Hadoop Installation 

In this project the very first step I did is installing Hadoop on my Mac, The process was bit complicated, It took me as much time as to do the code.
I referred to this link for installing Hadoop
https://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/

1.	Created a user Hadoop
2.	Installed and setup JAVA
3.	Installed Hadoop via brew
4.	Did SSH setup to connect without login 
5.	Configured Hadoop
a.	Core-site.xml
b.	Mapred-site.xml
c.	Hdfs-site.xml
d.	Hadoop-env.sh

If you don't want to take pain of Hadoop installation refer to the following link, it's installation of Hadoop via virtual box
https://docs.google.com/document/d/1v0zGBZ6EHap-Smsr3x3sGGpDW-54m82kDpPKC2M6uiY/pub

Skeleton of my programs

All programs have the same skeleton.
	Main Class
		Static Map class
			Map method
		Static Reduce class
			Reduce method
		Run method

Map method

The Map method is parsing the input data line by line and creating a several small chunks of the data and feeding that data to reduce function.

Reduce method

The reduce method is processing the data that Is coming from Map method, it is producing a new set of output and storing it into output file. 



Run method

	The run method is configuring the map and reduce method in the class.

Computing Selection by MapReduce
	In this program, I have a class selection which is the main class so it has main method, the run method is getting invoked from the main method and all the configuration and is done in this method.  
Map function is parsing through the input file city.txt given by the professor and getting the city name and the population of the respective city, and we are checking the condition if the population is greater than 300000 then the reduce function is writing the key of the respective city as an output.

Files:
Java file: selection.java 
Output file: selectionOutput
Jar file: selection.jar


Computing Projection by MapReduce

In this program, I have a class projection which is the main class so it has main method, the run method is getting invoked from the main method and all the configuration and is done in this method.
Map function is getting the names of the cities and corresponding districts of the cities where the reduce method is writing all the keys of the respective cities in the file. 

Files:
Java file: projection.java 
Output file: projectionOutput
Jar file: projection.jar


Computing Natural Join by MapReduce

In this program, I have a class join which is the main class so it has main method, the run method is getting invoked from the main method and all the configuration and is done in this method.
In the map function we are getting the countries and here language, then we are separating the countries which has English as an official language. In the reduce function we are writing the country which has English as there official language. 

Files:
Java file: join.java 
Output file: joinOutput
Jar file: join.jar




Computing Aggregation by MapReduce

	In this program, I have a class aggregation which is the main class so it has main method, the run method is getting invoked from the main method and all the configuration and is done in this method.
In the map function we are getting the list of district and cities and then the reduce function is counting the number of cities each district has.

Following is the screenshot of the output file
Files:
Java file: aggregation.java 
Output file: aggregationOutput
Jar file: aggregation.jar


