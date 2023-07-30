package Task4.Iter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class IterMain {

    public static void main(String[] args, Double init_score)
            throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        String in = args[0];
        String out = args[1];
        if (out.charAt(out.length() - 1) != '/')
            out = out + "/";

        conf.set("init_score", String.valueOf(init_score));
        // 保存init_score等待reducer取用

        // task1
        Job job1 = Job.getInstance(conf, "Task4_Iter");
        job1.setJarByClass(IterMain.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(IterMapper.class);
        job1.setReducerClass(IterReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job1, new Path(in));
        FileOutputFormat.setOutputPath(job1, new Path(out));
        job1.waitForCompletion(true);

    }
}
