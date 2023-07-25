package com.opstty.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeDistrictWritable implements Writable {
    private int year;
    private String district;

    public AgeDistrictWritable() {
    }

    public AgeDistrictWritable(int year, String district) {
        this.year = year;
        this.district = district;
    }

    public int getYear() {
        return year;
    }

    public String getDistrict() {
        return district;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeUTF(district);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        district = in.readUTF();
    }

    @Override
    public String toString() {
        return year + "\t" + district;
    }
}
