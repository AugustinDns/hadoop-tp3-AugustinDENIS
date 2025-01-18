package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationMapper extends Mapper<Object, Text, UserRecommendationKey, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split("\\s+");
        if (parts.length != 2) {
            System.err.println("Invalid input line: " + line);
            return;
        }

        String[] users = parts[0].split(",");
        if (users.length != 2) {
            System.err.println("Invalid user pair: " + parts[0]);
            return;
        }

        String user1 = users[0];
        String user2 = users[1];
        int commonConnections;
        try {
            commonConnections = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid common connections count: " + parts[1]);
            return;
        }

        UserRecommendationKey key1 = new UserRecommendationKey(new Text(user1), new Text(user2), commonConnections);
        UserRecommendationKey key2 = new UserRecommendationKey(new Text(user2), new Text(user1), commonConnections);

        context.write(key1, new Text());
        context.write(key2, new Text());
    }

}