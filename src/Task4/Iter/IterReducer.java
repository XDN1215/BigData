package Task4.Iter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, NullWritable> {

    public static Double p = 0.2;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String edge = "";
        Double score = 0.0;

        for (Text x : values) {
            // 两种类型：Harry Ron,0.66667|Hermione,0.33333 或 Harry 0.13334
            String value = x.toString();
            if (value.contains(","))
                edge = value;// 有逗号，表示是边集
            else
                score += Double.parseDouble(value);
            // 汇总该点从其它点处得到的分数
        }

        Configuration config = context.getConfiguration();
        String init_score = config.get("init_score");// 获取初始值
        score *= (1 - p);// 分数乘以1-p
        score += Double.parseDouble(init_score) * p;

        context.write(new Text(key + "!" + score.toString() + ":" + edge), NullWritable.get());

    }
}