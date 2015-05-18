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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LRandKeyMapper extends Mapper <LongWritable, Text, Text, Text> {

	public void map (LongWritable key, Text value, Context context )
	throws NumberFormatException,IOException,InterruptedException {

		/*put the value into an array of coordinates, ignore 0st (id), last 2 (energy, rmsd)*/
		String[] values = value.toString().split("\\s+");
		String id = values[0].toString();

		double[] x ,y ,z ;
		int rmsd_index = values.length-1;
		int num_atom = ((values.length -3)/3)-1; //number of atoms -1
		x = new double[(num_atom+1)];
		y = new double[(num_atom+1)];
		z = new double[(num_atom+1)];

		/*assume x, y, z in range [-4,4]*/
		double minx = -10.0;
		double miny = -10.0;
		double minz =-10.0;
		double maxx = 10.0;
		double maxy = 10.0;
		double maxz = 10.0;
		int digit = 15; //how many digits in the octkey
		boolean flag = true; //true means the range of octant is valid, min < max
		StringBuilder keys = new StringBuilder(); //store the actual octkey

		/*x,y,z double[] to store coordinates for x , y , and z. ii index for x array, xx index for values array*/
		int xx=1;
		for (int ii=0; ii<=num_atom;ii++){
			x[ii]=Double.valueOf(values[xx]).doubleValue();
			xx +=3;
		}
		int yy=2;
		for (int jj=0; jj<=num_atom; jj++){
			y[jj]=Double.valueOf(values[yy]).doubleValue();
			yy +=3;
		}
		int zz=3;
		for (int kk=0; kk<=num_atom; kk++){
			z[kk]=Double.valueOf(values[zz]).doubleValue();
			zz +=3;
		}
		/*for (int i=0;i<=num_atom;i++){
			System.out.format("the x,y,z coordinates are: %f, %f, %f.", x[i], y[i], z[i]);

		}*/

		if (rmsd_index >0){
			double rmsd = Double.valueOf(values[rmsd_index]).doubleValue();

			/*compute the 3 slopes: beta0,beta1,beta2 using linear regression*/
			double b0 = linear_regression_slop(x,y,(num_atom+1));
			double b1 = linear_regression_slop(y,z,(num_atom+1));
			double b2 = linear_regression_slop(x,z,(num_atom+1));

			/*compute octkey 10 digits
			 * the octkey is stored in the StringBuilder keys, octkey contains 15 digits, each digit represent the 
			 * octant after each dividion. The first digit is the octant id after first division, etc.*/
			keys=compute_octkey( minx,  maxx,  miny,  maxy, 
					 minz,  maxz,  b0,  b1,  b2,  digit,  flag);
			
			

			//Text outValue = new Text(Double.toString(b0) + ";" + Double.toString(b1) + ";" + Double.toString(b2) + ";" + Double.toString(rmsd));
			Text outValue = new Text(keys.toString());

			context.write(new Text(id), outValue);

		}

	}

	/* compute the slope of the linear regression line*/
	public double linear_regression_slop(double[]x, double[]y, int m){
        /*compute xbar and ybar m is the number of points
                 * index of x coordinates starts from 1, add 3
                 * index of y starts from 2, add 3
                 * index of z starts from 3, add 3*/
                double sumx = 0.0, sumy = 0.0;
                int i=0;
                while (i<m){
                        sumx += x[i];
                        sumy += y[i];
                        //sumx2 += x[i] * x[i];
                        i++;
                }

                double xbar = sumx/i;
                double ybar = sumy/i;

                /*compute summary statistics*/
                double xxbar = 0.0, yybar=0.0, xybar=0.0;
        for (int j = 0; j < i; j++) {
                xxbar += (x[j] - xbar) * (x[j] - xbar);
            yybar += (y[j] - ybar) * (y[j] - ybar);
            xybar += (x[j] - xbar) * (y[j] - ybar);

        }
        double beta1 = xybar / xxbar;
        return beta1;
    }



	/* compute the slope of the linear regression line*/

	public StringBuilder compute_octkey(double minx, double maxx, double miny, double maxy, 
			double minz, double maxz, double x, double y, double z, int digit, boolean flag){
		/*compute octkey 15 digits*/
		StringBuilder octkey = new StringBuilder();
		int count =0; //count how many digits are in the key
		while ((count <digit) && flag){
			int m0=0;
			int m1=0;
			int m2=0;
			double medx = minx + ((maxx-minx)/2);
			if (x>medx){
				m0=1;
				minx=medx;
			}else{
				maxx=medx;
			}
			double medy = miny + ((maxy-miny)/2);
			if (y>medy){
				m1=1;
				miny=medy;
			}else{
				maxy=medy;
			}
			double medz = minz + ((maxz-minz)/2);
			if (z>medz){
				m2=1;
				minz=medz;
			}else{
				maxz=medz;
			}
			/*calculate the octant using the formula m0*2^0+m1*2^1+m2*2^2*/
			int bit=m0+(m1*2)+(m2*4);
			octkey.append(bit);
			//System.out.format("the %d level of the octkey, the key is: %d.",count,bit);

			/*set the flag is the range is not valud*/
			if ((minx>=maxx)||(miny>=maxy)||(minz>=maxz)){
				flag=false;
			}

			count++;

		} 
		return octkey;
	}

}
