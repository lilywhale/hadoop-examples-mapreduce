package com.opstty.mapper;

import com.opstty.writable.AgeDistrictWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object, Text, NullWritable, AgeDistrictWritable> {
    private AgeDistrictWritable ageDistrictWritable = new AgeDistrictWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(";");
        if (columns.length >= 6 && !columns[5].isEmpty() && !columns[1].isEmpty()) {
            int ageValue = Integer.parseInt(columns[5].trim());
            String districtValue = columns[1].trim();
            ageDistrictWritable = new AgeDistrictWritable(ageValue, districtValue);
            context.write(NullWritable.get(), ageDistrictWritable);
        }
    }
}
