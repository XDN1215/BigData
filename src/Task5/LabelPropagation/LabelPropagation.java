package Task5.LabelPropagation;

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

public class LabelPropagation {
    public static void LabelPropagationMain(String out,int i)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration LabelPropagationConf = new Configuration();
        LabelPropagationConf.set("out", out);
        String labelFile = out + "labelFile_";
        String neighbourFile = out+"task3";
        LabelPropagationConf.set("labelFile", labelFile+(i));
        LabelPropagationConf.set("neighbourFile", neighbourFile);
        String isLPAOverFile = out + "isLPAOverFile";
        LabelPropagationConf.set("isLPAOverFile", isLPAOverFile);
        //
        Job LabelPropagationJob = new Job(LabelPropagationConf, "LabelPropagation");
        LabelPropagationJob.setJarByClass(LabelPropagation.class);
        LabelPropagationJob.setInputFormatClass(TextInputFormat.class);
        LabelPropagationJob.setMapperClass(LabelPropagationMapper.class);
        LabelPropagationJob.setMapOutputKeyClass(IntWritable.class);
        LabelPropagationJob.setMapOutputValueClass(Text.class);
        LabelPropagationJob.setPartitionerClass(TotalOrderPartitioner.class);
        LabelPropagationJob.setReducerClass(LabelPropagationReducer.class);
        LabelPropagationJob.setOutputKeyClass(Text.class);
        LabelPropagationJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(LabelPropagationJob, new Path(labelFile+(i)));
        FileOutputFormat.setOutputPath(LabelPropagationJob, new Path(labelFile+(i+1)));
        LabelPropagationJob.waitForCompletion(true);
    }
}