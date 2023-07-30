package Task5.LabelPropagation;

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
import java.util.Iterator;

public class LabelPropagationReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //randX name,label----->name,label
        Iterator<Text> iterator = values.iterator();
        //处理每一项name,label
        for (; iterator.hasNext(); ) {
            String[] splits = iterator.next().toString().split(",");
            Text word1 = new Text();
            //将name,label直接写入最后结果
            word1.set(splits[0] + "," + splits[1]);
            context.write(word1, NullWritable.get());

            //将name,label写入本地labelLocalFile，方便mapper的setup()直接读取此文件建立labelMap
            Configuration conf = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(conf);
            String labelLocalFile = conf.get("out") + "labelLocalFile";
            FSDataOutputStream fsDataOutputStream = null;
            if (!fileSystem.exists(new Path(labelLocalFile))) {
                fsDataOutputStream = fileSystem.create(new Path(labelLocalFile));
            } else {
                fsDataOutputStream = fileSystem.append(new Path(labelLocalFile));
            }
            fsDataOutputStream.write(new String(splits[0] + "," + splits[1] + "\n").getBytes());
            fsDataOutputStream.close();
        }
    }
}
