package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DistinctSpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {
    private Set<String> speciesSet = new HashSet<>();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (columns.length >= 4) {
            String speciesStr = columns[3].trim();
            speciesSet.add(speciesStr);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String species : speciesSet) {
            context.write(new Text(species), NullWritable.get());
        }
    }
}


