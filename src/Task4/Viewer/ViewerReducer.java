package Task4.Viewer;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ViewerReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text x : values) {
            Double score = -key.get();
            DecimalFormat df = new DecimalFormat("0.00");
            //保留两位小数输出
            context.write(x, new Text(df.format(score)));
        }
    }
}