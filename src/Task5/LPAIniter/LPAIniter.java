package Task5.LPAIniter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;

public class LPAIniter {
    public static void LPAIniterMain(String out)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration LPAIniterConf = new Configuration();
        LPAIniterConf.set("out", out);
        String labelFile = out + "labelFile_0";
        String neighbourFile = out + "task3";
        //
        Job LPAIniterJob = new Job(LPAIniterConf, "LPAIniter");
        LPAIniterJob.setJarByClass(LPAIniter.class);
        LPAIniterJob.setInputFormatClass(TextInputFormat.class);
        LPAIniterJob.setMapperClass(LPAIniterMapper.class);
        LPAIniterJob.setMapOutputKeyClass(IntWritable.class);
        LPAIniterJob.setMapOutputValueClass(Text.class);
        LPAIniterJob.setPartitionerClass(TotalOrderPartitioner.class);
        LPAIniterJob.setReducerClass(LPAIniterReducer.class);
        LPAIniterJob.setOutputKeyClass(Text.class);
        LPAIniterJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(LPAIniterJob, new Path(neighbourFile));
        FileOutputFormat.setOutputPath(LPAIniterJob, new Path(labelFile));
        LPAIniterJob.waitForCompletion(true);
    }
}