package edu.wpi.ds503.q3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public  class KMeansCombiner  extends Reducer<PointWritable, PointsAverageWritable,PointWritable, PointsAverageWritable> {

	public void reduce(PointWritable centroidid, Iterable<PointsAverageWritable> points, Context context)
			throws IOException, InterruptedException {
		float sumx = 0;
		float sumy = 0;
		int num = 0;
		for (PointsAverageWritable point : points) {
			sumx += (point.getAvg_x().get() * point.getNum().get());
			sumy += (point.getAvg_y().get() * point.getNum().get());
			num += point.getNum().get();
		}

		float centerx = sumx/num;
		float centery = sumy/num;
		PointsAverageWritable pointsAverageWritable = new PointsAverageWritable(centerx/(float)num, centery/(float)num , num);

		context.write(centroidid , pointsAverageWritable);
	}
}