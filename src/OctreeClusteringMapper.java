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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class OctreeClusteringMapper extends Mapper <LongWritable, Text, Text, Text> {

	int level= 0;

	/*setup runs once for each map task before the map function, get the current level to explore 
	 * in the tree.*/
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		String level_s = conf.getRaw("level");
		level = Integer.parseInt(level_s);


	}
	public void map (LongWritable key, Text value, Context context )
			throws IOException, InterruptedException{

		/* compute the id of the octant in level m that contains the octkey, 
		 * the id of the octant is the first m digits of the octkey
		 * level is set by the Driver.java depending on whether a dense enough octant
		 * is found in the previous job. level=m*/
		String[] values = value.toString().split("\\s+");
		int values_len = values.length;
		if (values_len ==2){
			String octkey = values[1].toString();

			/*get the 1st couple digits of each octkey(level), 0 inclusive, level exclusive*/
			String outKey_s = octkey.substring(0, level);


			String outValue_s = "1";
			context.write(new Text(outKey_s), new Text(outValue_s));

		}
	}

}
