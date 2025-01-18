package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class UserPairReducer extends Reducer<UserPair, Text, UserPair, Text> {

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> commonRelations = new HashSet<>();
        for (Text value : values) {
            commonRelations.add(value.toString());
        }

        context.write(key, new Text(String.valueOf(commonRelations.size())));
    }
}
