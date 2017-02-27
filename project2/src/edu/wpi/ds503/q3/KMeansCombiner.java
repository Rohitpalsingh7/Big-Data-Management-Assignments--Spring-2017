package edu.wpi.ds503.q3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public  class KMeansCombiner  extends Reducer<PointWritable, PointWritable,PointWritable, PointsAverageWritable> {

	public void reduce(PointWritable centroidid, Iterable<PointWritable> points, 
			Context context
			) throws IOException, InterruptedException {

		int num = 0;
		int centerx=0;
		int centery=0;
		for (PointWritable point : points) {
			num++;
			IntWritable X = point.getx();
			IntWritable Y = point.gety();
			int x = X.get();
			int y = Y.get();
		
			centerx += x;
			centery += y;
		}

		centerx = centerx/num;
		centery = centery/num;
		PointsAverageWritable pointsAverageWritable = new PointsAverageWritable(centerx/(float)num, centery/(float)num , num);
		
		context.write(centroidid , pointsAverageWritable);
	}
}