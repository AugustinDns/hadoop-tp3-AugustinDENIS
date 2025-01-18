package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RecommendationReducer extends Reducer<UserRecommendationKey, Text, Text, Text> {

    @Override
    protected void reduce(UserRecommendationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> recommendations = new ArrayList<>();

        for (Text value : values) {
            recommendations.add(value.toString());
        }

        Collections.sort(recommendations);

        StringBuilder topRecommendations = new StringBuilder();
        int count = 0;
        for (String recommendation : recommendations) {
            topRecommendations.append(recommendation).append(", ");
            count++;
            if (count == 5) {
                break;
            }
        }

        if (topRecommendations.length() > 0) {
            topRecommendations.setLength(topRecommendations.length() - 2);
            context.write(new Text(key.toString()), new Text(topRecommendations.toString()));
        }
    }
}
