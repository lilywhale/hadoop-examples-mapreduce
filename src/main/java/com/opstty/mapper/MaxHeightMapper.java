package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private DoubleWritable height = new DoubleWritable();
    private Text kind = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (columns.length >= 7 && !columns[6].isEmpty()) {
            String kindStr = columns[2].trim();
            Double heightValue = Double.parseDouble(columns[6].trim());
            kind.set(kindStr);
            height.set(heightValue);
            context.write(kind, height);
        }
    }
}

