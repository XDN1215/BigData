package Task5.LPAIniter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class LPAIniterReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String name = value.toString().split(",")[0];
            String label = value.toString().split(",")[1];
            context.write(new Text(name + "," + label), NullWritable.get());

            Configuration conf = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(conf);
            String labelLocalFile = conf.get("out") + "labelLocalFile";
            FSDataOutputStream fsDataOutputStream = null;
            if (!fileSystem.exists(new Path(labelLocalFile))) {
                fsDataOutputStream = fileSystem.create(new Path(labelLocalFile));
            } else {
                fsDataOutputStream = fileSystem.append(new Path(labelLocalFile));
            }
            fsDataOutputStream.write(new String(name + "," + label + "\n").getBytes());
            fsDataOutputStream.close();
        }
    }
}
