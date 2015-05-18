# Octree-based Clustering in Hadoop 

This repository contains a linear clustering approach for large datasets of 
molecular geometries produced by high-throughput molecular dynamics simulations 
(e.g., protein folding and protein-ligand docking simulations). The clustering 
is adapted for MapReduce and implemented in Hadoop. 

For detailed design and implementaion aspects, please refer to our following papers:

Trilce Estrada, Boyu Zhang, Pietro Cicotti, Roger Armen, and Michela Taufer: 
A Scalable and Accurate Method for Classifying Protein-Ligand Binding Geometries 
using a MapReduce Approach. Computers in Biology and Medicine, 
42(7): 758-771, 2012.

Trilce Estrada, Boyu Zhang, Pietro Cicotti, Roger Armen, and  Michela Taufer: 
Reengineering High-throughput Molecular Datasets for Scalable Clustering using 
MapReduce. In Proceedings of the 14th IEEE International Conference on High 
Performance Computing and Communications (HPCC), June 2012, Liverpool, England, UK.

Boyu Zhang, Trilce Estrada, Pietro Cicotti, and Michela Taufer. On Efficiently 
Capturing Scientific Properties in Distributed Big Data without Moving the Data 
- A Case Study in Distributed Structural Biology using MapReduce. In the 
Proceedings of the 16th IEEE International Conferences on Computational Science 
and Engineering (CSE), December 2013, Sydney, Australia.

## Directories and files 

### data
Contains two text files serve as sample input to the Hadoop program.

* ligand_small.txt 

Contains 10 ligand conformtions, one conformation per line. Each line is in the 
format of [ligand_id x1 y1 z1 x2 y2 z2 … xN yN ZN energy rmsd], in which 
ligand_id is the ligand conformation ID, x1 y1 z1 x2 y2 z2 … xN yN ZN are the 
coordinates in the Cartesian space of the N ligand atoms, energy is the 
conformation energy, and rmsd is the RMSD from the crystal structure in LPDB. 
From a representation point of view, ligand_id is represented as the string 
type and the others values in each line are represented as the double type.

* ligand_1a9u.txt 

Contains 76889 ligand conformations with the same format.

### src
Contains the source files for the program.

* Driver.java

Contains the main function, parses the command line input, sets the parameters 
that control the program behavior, calls map and reduce functions.

* LRandKeyMapper.java

Contains the map function that transforms ligand conformations to 3-D metadata 
points using projection and linear interpulation.

* LRandKeyReducer.java

Contains the identify reduce function.

* OctreeClusteringMapper.java

Contains the map function that search one level of the octree for dense octants.

* OctreeClusteringReducer.java

Contains the reduce function that computes the global densities of all the 
octants in one specific level of the octree.

## Compilation instructions 

### Supported platform
GNU/Linux

### Required software
* Java must be installed. The code requires Java 7 or a late version of Java 6.
* Hadoop framework must be installed. The code is tested against Hadoop version 
0.20.2 and 2.4.0.

### Compilation instructions using Eclipse

* Create a new project in Eclipse:
Open Eclipse => File => New Java Project => Give a project name (e.g.: 
Octree_Clustering) => Next => Click on the tab “Libraries” => Add External Jars 
=> Navigate to the dir “/path/to/hadoop-2.4.0” => Add following jars:
```
 ~/hadoop-2.4.0/share/hadoop/common/hadoop-common-2.4.0.jar
 ~/hadoop-2.4.0/share/hadoop/hdfs/hadoop-hdfs-2.4.0.jar
 ~/hadoop-2.4.0/share/hadoop/mapreduce/*.jar
 ~/hadoop-2.4.0/share/hadoop/yarn/*jar
```
=> Finish.

* Copy the source files in the src directory which the Eclipse project creats, 
and refresh the project in Eclipse.

You should see the source files from Eclipse.

### Compilation instructions using command line
The following instructions are tested against hadoop-2.4.0.

* Export CLASSPATH
```
[hadoop@localhost Octree_Clustering]$ export CLASSPATH=`yarn classpath`
```
* Compile java files
```
[hadoop@localhost Octree_Clustering]$ javac -classpath $CLASSPATH -d . *java
```
* Make the jar file
```
[hadoop@localhost Octree_Clustering]$ jar cvf octree.jar *class
```

### Execution instructions 

* Create a jar file:
Change directory to the Octree_Clustering, run the command:
```
[hadoop@localhost Octree_Clustering]$ jar cvf octree.jar -C bin .
added manifest
adding: LRandKeyReducer.class(in = 2076) (out= 774)(deflated 62%)
adding: Driver.class(in = 3006) (out= 1624)(deflated 45%)
adding: LRandKeyMapper.class(in = 4636) (out= 2385)(deflated 48%)
```

* Copy the octree.jar to the hadoop installation dir:
```
[hadoop@localhost Octree_Clustering]$ cp octree.jar ~/hadoop-2.4.0
```

* Start the hadoop daemons & hadoop job history server from the 
hadoop installation dir:
```
[hadoop@localhost hadoop-2.4.0]$ sbin/start-all.sh
This script is Deprecated. Instead use start-dfs.sh and start-yarn.sh
14/05/27 10:10:00 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Starting namenodes on [localhost]
localhost: starting namenode, logging to /home/hadoop/hadoop-2.4.0/logs/hadoop-hadoop-namenode-localhost.localdomain.out
localhost: starting datanode, logging to /home/hadoop/hadoop-2.4.0/logs/hadoop-hadoop-datanode-localhost.localdomain.out
Starting secondary namenodes [0.0.0.0]
0.0.0.0: starting secondarynamenode, logging to /home/hadoop/hadoop-2.4.0/logs/hadoop-hadoop-secondarynamenode-localhost.localdomain.out
14/05/27 10:10:21 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
starting yarn daemons
starting resourcemanager, logging to /home/hadoop/hadoop-2.4.0/logs/yarn-hadoop-resourcemanager-localhost.localdomain.out
localhost: starting nodemanager, logging to /home/hadoop/hadoop-2.4.0/logs/yarn-hadoop-nodemanager-localhost.localdomain.out
[hadoop@localhost hadoop-2.4.0]$ sbin/mr-jobhistory-daemon.sh start historyserver
starting historyserver, logging to /home/hadoop/hadoop-2.4.0/logs/mapred-hadoop-historyserver-localhost.localdomain.out
```
* Verify that everything starts successfully:
```
[hadoop@localhost hadoop-2.4.0]$ jps
3626 NodeManager
3403 SecondaryNameNode
3255 DataNode
3537 ResourceManager
4028 Jps
3141 NameNode
3958 JobHistoryServer
```

* Put the ligand_small.txt input to hdfs:
```
[hadoop@localhost hadoop-2.4.0]$ bin/hdfs dfs -mkdir /input
14/05/29 04:01:47 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
[hadoop@localhost hadoop-2.4.0]$ bin/hdfs dfs -copyFromLocal ligand_small.txt /input
14/05/29 04:02:16 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
```

* Run the octree clustering program:
```
[hadoop@localhost hadoop-2.4.0]$ bin/hadoop jar octree.jar Driver -input /input -output /output -octant_size 2 -ini_level 8 -num_reduce_1 2 -num_reduce_2 1

```
Note: “-input /input” specifies the input path in hdfs, “-output /output” specifies the output in hdfs, “-octant_size 2” specifies the threshold for the density is 2, “-ini_leve 8” specifies the 1st MapReduce job counts the densities in level 8, “-num_reduce_1 2” specifies the number of reducers for the first step is 2, “-num_reduce_2 1” specifies the number of reducers for the second step is 1.

You will see information on 5 MapReduce jobs from the standard output.

* Copy the results from hdfs to local disk: 
```
[hadoop@localhost hadoop-2.4.0]$ bin/hdfs dfs -copyToLocal /output .
```
* Check out the output in output dir (local):
```
[hadoop@localhost hadoop-2.4.0]$ cat output/output0/part-r-00000
```
* Rerun the octree.jar with a larger input dataset ligand_1a9u.txt:

Note: if you wish to use the /output on hdfs as the output of the program running on ligand_1a9u.txt, you need to remove the /output dir from hdfs before to rerun. Or you can simply let the output of running on 1a9u.txt to another directory on hdfs, for example /output_1a9u.



