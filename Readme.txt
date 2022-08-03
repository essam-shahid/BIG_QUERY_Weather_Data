*******

We have a python file with 3 functions defined. Once we have the url, I have kept a dynamic date which will extract URL for day -1 everytime 
our pipeline will execute.

The first function will return a json object, the second will flatten it and return a dataframe. The third will load it into the files.

*Please note, I didn't have DML rights on Bigquery, hence I made a work around. I created a staging table and created a view on top of it.
I have another python file where we loaded the data into an empty table and loaded into a core table.
Main_Core.py*

3 pytests were included. 1 to check connectivity, 2 to check json keys are correct and third to check placing of keyfile.json for Big query connectivity.
More can be added.

1) For this, I have created a docker file which will execute the pipeline.
2) For you to run the file, kindly execute the "Assignment_2.sql" ddl on your google big query dataset. Make sure the name is same as it is being used in in the pipeline.
3) Once that is done,please make sure keyfile.json file is placed in the following directory:
	\etl
	This is required for authentication
	
4) Once that is done,execute the shell "setup.sh', which will create a docker and execute the pipeline and close the docker.
2.