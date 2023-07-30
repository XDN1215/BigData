package Task5.LPAViewer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LPAViewerReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String name_list = "";
        for (Text value : values) {
            name_list += value.toString() + ',';
        }
        name_list = name_list.substring(0, name_list.length() - 1);
        context.write(key, new Text(name_list));
    }
}