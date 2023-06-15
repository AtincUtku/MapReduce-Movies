# CENG 495 Cloud Computing Spring 2022–2023 HW - 3

This README outlines the process of building and running the MapReduce jobs for CENG 495 Cloud Computing homework. We have five different tasks each corresponding to a MapReduce job: `total`, `average`, `employment`, `ratescore`, and `genrescore`.

## Requirements
Ensure you have the following installed on your machine:

- Java 8 or higher
- Apache Hadoop 2.x or higher

The input data set should be a TSV file converted from the original CSV file.

## .csv to .tsv Conversion
Since there are values with "," in movies.csv, I decided to convert the dataset to .tsv with csv_to_tsv.py. 
You can convert movies.csv to movies.tsv with just running "python3 csv_to_tsv.py". Please keep python script and movies.csv in the same directory.
The script will create movies.tsv in the current directory.
If the .csv file's name is different from movies.csv you can change it from the script. I assumed that it will always be like it.

## Building
Compile the java files and create a jar file as below:

# compile java files
hadoop com.sun.tools.javac.Main *.java

# create jar
jar cf Hw3.jar *.class

## Pre-Processing HDFS
The movies.tsv was put to Hadoop file system with running:
"hdfs dfs -put movies.tsv / " (In my case it was put to root directory since it was easier to manage.)

Also in MapReduce functions I passed the films if they contained empty field to process. I realized this in employment part. One of the film's actor field is empty and my script recognized it as a actor with no name. Therefore, I decided to ignore this kind of anomalies.

## Running
The following commands will run each MapReduce job:

1 Total runtime of the movies
hadoop jar Hw3.jar total /movies.tsv /output_total

2 Average runtime of the movies
hadoop jar Hw3.jar average /movies.tsv /output_average

3 Employment count for each actor
hadoop jar Hw3.jar employment /movies.tsv /output_employment

4 Average number of IMDb votes on G, PG, PG-13 and R rated movies
hadoop jar Hw3.jar ratescore /movies.tsv /output_ratescore

5 Average IMDb score of genres that have more than 9 movies
hadoop jar Hw3.jar genrescore /movies.tsv /output_genrescore

## Outputs
After running above commands related directories are created in hadoop file system. When we run "hdfs dfs -ls":

utku@Atincs-MBP Hw3 % hdfs dfs -ls /
2023-06-02 05:13:19,977 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 6 items
-rw-r--r--   1 utku supergroup    1337160 2023-06-02 03:04 /movies.tsv
drwxr-xr-x   - utku supergroup          0 2023-06-02 04:38 /output_average
drwxr-xr-x   - utku supergroup          0 2023-06-02 04:42 /output_employment
drwxr-xr-x   - utku supergroup          0 2023-06-02 04:46 /output_genrescore
drwxr-xr-x   - utku supergroup          0 2023-06-02 04:45 /output_ratescore
drwxr-xr-x   - utku supergroup          0 2023-06-02 03:06 /output_total

For instance after running "hdfs dfs -ls /output_average":

utku@Atincs-MBP Hw3 % hdfs dfs -ls /output_average
2023-06-02 05:15:13,894 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 2 items
-rw-r--r--   1 utku supergroup          0 2023-06-02 04:38 /output_average/_SUCCESS
-rw-r--r--   1 utku supergroup         26 2023-06-02 04:38 /output_average/part-r-00000

The output is in part-r-00000.

If we run "hdfs dfs -cat /output_average/part-r-00000":

utku@Atincs-MBP Hw3 % hdfs dfs -cat /output_average/part-r-00000
2023-06-02 05:17:03,604 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
average_runtime 107.26161

I redirected the outputs to the related .txt files in "outputs" folder. Also outputs can be observed in raw form, from "outputs_hdfs" folder which stores the
raw versions fetched from hadoop file system with "hdfs dfs -get <hdfs_directory_name> <local_move_directory>".

Atınç Utku Alparslan
2380061
