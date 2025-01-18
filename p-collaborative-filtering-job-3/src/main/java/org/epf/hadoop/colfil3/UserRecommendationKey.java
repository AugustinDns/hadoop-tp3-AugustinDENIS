package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserRecommendationKey implements WritableComparable<UserRecommendationKey> {
    private Text user;
    private Text recommendedUser;
    private int commonConnectionsCount;

    public UserRecommendationKey() {
        this.user = new Text();
        this.recommendedUser = new Text();
    }

    public UserRecommendationKey(Text user, Text recommendedUser, int commonConnectionsCount) {
        this.user = user;
        this.recommendedUser = recommendedUser;
        this.commonConnectionsCount = commonConnectionsCount;
    }

    public Text getUser() {
        return user;
    }

    public void setUser(Text user) {
        this.user = user;
    }

    public Text getRecommendedUser() {
        return recommendedUser;
    }

    public void setRecommendedUser(Text recommendedUser) {
        this.recommendedUser = recommendedUser;
    }

    public int getCommonConnectionsCount() {
        return commonConnectionsCount;
    }

    public void setCommonConnectionsCount(int commonConnectionsCount) {
        this.commonConnectionsCount = commonConnectionsCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        user.write(out);
        recommendedUser.write(out);
        out.writeInt(commonConnectionsCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user.readFields(in);
        recommendedUser.readFields(in);
        commonConnectionsCount = in.readInt();
    }

    @Override
    public int compareTo(UserRecommendationKey other) {
        int cmp = Integer.compare(other.commonConnectionsCount, this.commonConnectionsCount);
        if (cmp != 0) {
            return cmp;
        }
        return this.recommendedUser.compareTo(other.recommendedUser);
    }

    @Override
    public String toString() {
        return recommendedUser.toString() + " (" + commonConnectionsCount + ")";
    }
}
