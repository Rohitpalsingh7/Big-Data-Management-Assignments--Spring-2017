package edu.wpi.ds503.q3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yousef fadila on 26/02/2017.
 */
public class PointsAverageWritable implements Writable {

    private FloatWritable avg_x;
    private FloatWritable avg_y;

    private IntWritable num;

    public PointsAverageWritable(float avgx, float avgy, int num) {
        set(new FloatWritable(avgx), new FloatWritable(avgy), new IntWritable(num));
    }

    public PointsAverageWritable() {
        set(new FloatWritable(0), new FloatWritable(0), new IntWritable(0));
    }

    private void set(FloatWritable x, FloatWritable y, IntWritable num) {
        this.avg_x = x;
        this.avg_y = y;
        this.num = num;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        avg_x.write(dataOutput);
        avg_y.write(dataOutput);
        num.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        avg_x.readFields(dataInput);
        avg_y.readFields(dataInput);
        num.readFields(dataInput);
    }

    public FloatWritable getAvg_x() {
        return avg_x;
    }

    public FloatWritable getAvg_y() {
        return avg_y;
    }

    public IntWritable getNum() {
        return num;
    }
}
