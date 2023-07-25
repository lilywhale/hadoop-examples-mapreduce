package com.opstty.job;

import com.opstty.mapper.DistinctArrondissementsMapper;
import com.opstty.reducer.DistinctArrondissementsReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DistinctArrondissements {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: distinctarrondissements <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "distinctarrondissements");
        job.setJarByClass(DistinctArrondissements.class);
        job.setMapperClass(DistinctArrondissementsMapper.class);
        job.setReducerClass(DistinctArrondissementsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        long distinctArrondissementsCount = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
                "REDUCE_OUTPUT_RECORDS").getValue();

        System.out.println("Number of distinct arrondissements containing trees: " + distinctArrondissementsCount);

        System.exit(exitCode);
    }
}


