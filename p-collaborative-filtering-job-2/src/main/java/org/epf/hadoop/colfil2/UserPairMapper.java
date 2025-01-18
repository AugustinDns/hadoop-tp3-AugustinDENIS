package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class UserPairMapper extends Mapper<LongWritable, Text, UserPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");

        if (tokens.length != 2) {
            return;
        }

        String userId = tokens[0];
        String[] relations = tokens[1].split(",");

        for (int i = 0; i < relations.length; i++) {
            for (int j = i + 1; j < relations.length; j++) {
                String user1 = userId;
                String user2 = relations[i];
                String user3 = relations[j];

                UserPair pair1 = new UserPair(user1, user2);
                UserPair pair2 = new UserPair(user1, user3);

                context.write(pair1, new Text(user3));
                context.write(pair2, new Text(user2));
            }
        }
    }
}
