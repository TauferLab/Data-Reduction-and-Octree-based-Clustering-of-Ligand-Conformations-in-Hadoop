/*
Copyright (c)
	2015 by The University of Delaware
	Contributors: Boyu Zhang, Michela Taufer
	Affiliation: Global Computing Laboratory, Michela Taufer PI
	Url: http://gcl.cis.udel.edu/, https://github.com/TauferLab

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

	1. Redistributions of source code must retain the above copyright notice, 
	this list of conditions and the following disclaimer.

	2. Redistributions in binary form must reproduce the above copyright notice,
	this list of conditions and the following disclaimer in the documentation
	and/or other materials provided with the distribution.

	3. If this code is used to create a published work, one of the following 
	papers must be cited.
		
		Trilce Estrada, Boyu Zhang, Pietro Cicotti, Roger Armen, and 
		Michela Taufer: A Scalable and Accurate Method for Classifying 
		Protein-Ligand Binding Geometries using a MapReduce Approach. 
		Computers in Biology and Medicine, 42(7): 758-771, 2012.

		Trilce Estrada, Boyu Zhang, Pietro Cicotti, Roger Armen, and 
		Michela Taufer: Reengineering High-throughput Molecular Datasets for 
		Scalable Clustering using MapReduce. In Proceedings of the 14th IEEE 
		International Conference on High Performance Computing and 
		Communications (HPCC), June 2012, Liverpool, England, UK.

		Boyu Zhang, Trilce Estrada, Pietro Cicotti, and Michela Taufer. On 
		Efficiently Capturing Scientific Properties in Distributed Big Data 
		without Moving the Data - A Case Study in Distributed Structural Biology 
		using MapReduce. In the Proceedings of the 16th IEEE International 
		Conferences on Computational Science and Engineering (CSE), December 
		2013, Sydney, Australia.

	4.  Permission of the PI must be obtained before this software is used
	for commercial purposes.  (Contact: taufer@acm.org)

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*/

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class Driver {

	/*read command line arguments
	 * -input input_path -output output_path -octant_size 100 -num_reduce_1 8 -num_reduce_2 8*/
	static String inputPath = null;
	static String outputPath = null;
	static String octantSize = "500";
	static int numReduceTasksOne = 2;
	static int numReduceTasksTwo = 2;
	static String iniLevel = "8";


	private static void ReadArguments(String[] commandLineArgs){
		/*read the command line and parse them, print usage message if not using correctly*/
		int size = commandLineArgs.length;

		for (int i = 0; i<size; i+=2){

			if (commandLineArgs[i].equals("-input")){
				inputPath = commandLineArgs[i+1];
			}else{
				if (commandLineArgs[i].equals("-output")){
					outputPath = commandLineArgs[i+1];
				}else{
					if (commandLineArgs[i].equals("-octant_size")){
						octantSize = commandLineArgs[i+1];
					}else{
						if (commandLineArgs[i].equals("-num_reduce_1")){
							numReduceTasksOne = Integer.valueOf(commandLineArgs[i+1]).intValue();
						}else{
							if (commandLineArgs[i].equals("-num_reduce_2")){
								numReduceTasksTwo = Integer.valueOf(commandLineArgs[i+1]).intValue();
							}else{
								if (commandLineArgs[i].equals("-ini_level")){
									iniLevel = commandLineArgs[i+1];
								}else{
									System.out.println("please use the format: -input input_path -output output_path -octant_size 100" +
									"-num_reduce_1 2 -num_reduce_2 2 -ini_level 8");
									System.exit(0);
								}
							}
						}
					}
				}
			}
		}//for

	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{

		/*reda arguments before proceed*/
		ReadArguments(args);

		/*after reading arguments, set the correct parameter, and start the job*/

		Configuration conf = new Configuration();
		Job job = new Job(conf, "octree");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*step 1: from data (ligand coordiantes) to metadata (3d point)
		 * map: input key: line number of each line. input value: line content (ligand_id, x,y,z,..., energy, rmsd)
		 *      output key: ligand_id. output value: octkey;rmsd
		 * reduce: input key: ligand_id. input value: list<octkey;rmsd> that all have the same ligand_id
		 *         output key: ligand_id, output value: octkey;rmsd*/

		job.setMapperClass(LRandKeyMapper.class);
		job.setJarByClass(Driver.class);
		job.setReducerClass(LRandKeyReducer.class);


		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path (outputPath.concat("/output0")));



		job.setNumReduceTasks(numReduceTasksOne);


		job.waitForCompletion(true);

		System.out.println("the reduction from 3d ligands to a single point in space is completed....");

		//////////////////////////////
		//////second job/////////////
		///////////////////////////////

		Job[] jobPool = new Job[10];

		String level = iniLevel;//initial level of the search of density
		int level_i = Integer.parseInt(level);

		int j = 0; //loop variable 

		int min_limit = 0;
		int max_limit = 16;

		/*initially set the level and octant size (first couple digits that have member >= 500)*/
		conf.set("level", level);//the level of octants to serach
		conf.set("octant.size", octantSize); //the density threshold of the octants

		System.out.println("the search for most compact octant in deepest level starts, the initial search level is:  "+ level);
		System.out.println("the ocatant.size is: "+ octantSize);
		while ((min_limit+1)<max_limit){
			System.out.println("the current level is : "+ level_i);


			jobPool[j]= new Job(conf, "octkey_clustering");


			jobPool[j].setOutputKeyClass(Text.class);
			jobPool[j].setOutputValueClass(Text.class);

			/*step 2: octree clustering - search for the deepest octnat that contains more points > octantSize
			 * map: input key: line number of each line. input value: line content (ligand_id octkey;rmsd)
			 *      output key: octkey[1..i]. output value: 1
			 * reduce: input key: octkey[1..i]. input value: list<1> that have the same octkey[1..i]
			 *         output key: octkey[1..i]. output value: density of the octant with id=octkey[1..i]*/
			
			jobPool[j].setMapperClass(OctreeClusteringMapper.class);
			jobPool[j].setJarByClass(Driver.class);
			//jobPool[j].setCombinerClass(Test.class);
			jobPool[j].setReducerClass(OctreeClusteringReducer.class);


			String inputPath2 = outputPath.concat("/output0");
			String outputPath2 = outputPath.concat("/output");


			FileInputFormat.setInputPaths(jobPool[j], new Path(inputPath2));
			FileOutputFormat.setOutputPath(jobPool[j], new Path (outputPath2+(j+1)));
			jobPool[j].setNumReduceTasks(numReduceTasksTwo);


			jobPool[j].waitForCompletion(true);

			Counters countersJ = jobPool[j].getCounters();

			Counter counter_i = countersJ.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS");
			long writeBytes = counter_i.getValue();
			/*uncomment the following code to see all the avialble
			 * groups and counters built-in hadoop*/
			
			/*Iterable<String> groups = countersJ.getGroupNames();
			System.out.println("the group name of countesrr: "+groups);
			Iterator<String> groups_i = groups.iterator();
			while (groups_i.hasNext()){
				String na = groups_i.next().toString();//group name
				CounterGroup cgrp=countersJ.getGroup(na);
				Iterator<Counter> gi = cgrp.iterator();
				while (gi.hasNext())
					System.out.println("the counters in "+na+" is: "+gi.next().getName());
			}
			System.out.println("The reduce output record is: "+writeBytes);*/

			

			if (writeBytes==0){//no octant with enough density, branch up on the tree

				max_limit = level_i;
				level_i = (int) Math.floor((min_limit+max_limit)/2);
				System.out.println("the current level is false");


			}else{//there is octant, branch down on the tree

				min_limit = level_i;
				level_i = (int) Math.floor((min_limit + max_limit)/2);
				System.out.println("the current level is "+ "true");


			}
			level = String.valueOf(level_i);
			conf.set("level", level);

			System.out.println("the next level is : "+ level);



			j++;
		}




	}



}

