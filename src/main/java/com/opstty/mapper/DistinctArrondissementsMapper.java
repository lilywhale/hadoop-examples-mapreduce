package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistinctArrondissementsMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text arrondissement = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (columns.length >= 2) {
            String arrondissementStr = columns[1].trim();
            arrondissement.set(arrondissementStr);
            context.write(arrondissement, NullWritable.get());
        }
    }
}

