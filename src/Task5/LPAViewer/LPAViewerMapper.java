package Task5.LPAViewer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LPAViewerMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[]tmp=value.toString().split(",");
        context.write(new Text(tmp[1]), new Text(tmp[0]));
    }
}