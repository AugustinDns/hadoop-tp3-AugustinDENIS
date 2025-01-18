package org.epf.hadoop.colfil1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class ColFilMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    @Override
    protected void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        System.out.println("Mapping: " + value.getId1() + " -> " + value.getId2());
        context.write(new Text(value.getId1()), new Text(value.getId2()));
        context.write(new Text(value.getId2()), new Text(value.getId1()));
    }

}
