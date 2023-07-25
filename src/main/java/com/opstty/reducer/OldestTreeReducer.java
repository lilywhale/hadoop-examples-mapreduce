package com.opstty.reducer;

import com.opstty.writable.AgeDistrictWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<NullWritable, AgeDistrictWritable, NullWritable, AgeDistrictWritable> {
    private AgeDistrictWritable oldestTreeInfo = null;

    public void reduce(NullWritable key, Iterable<AgeDistrictWritable> values, Context context) throws IOException, InterruptedException {
        int minYear = Integer.MAX_VALUE;
        String districtName = null;

        for (AgeDistrictWritable value : values) {
            if (value.getYear() < minYear) {
                minYear = value.getYear();
                districtName = value.getDistrict();
            }
        }

        if (districtName != null) {
            int age = 2023 - minYear; // Calculate age
            oldestTreeInfo = new AgeDistrictWritable(age, districtName);
            context.write(NullWritable.get(), oldestTreeInfo);
        }
    }
}



