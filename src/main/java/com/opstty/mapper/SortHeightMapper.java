package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<Object, Text, DoubleWritable, NullWritable> {
    private DoubleWritable height = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (columns.length >= 7 && !columns[6].isEmpty()) {
            Double heightValue = Double.parseDouble(columns[6].trim());
            height.set(heightValue);
            context.write(height, NullWritable.get());
        }
    }
}

