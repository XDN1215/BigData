package Task5.LPAViewer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class LPAViewer {
    public static void LPAViewerMain(String out,int i)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration LPAViewerConf = new Configuration();
        //
        Job LPAViewerJob = new Job(LPAViewerConf, "LPAViewer");
        LPAViewerJob.setJarByClass(LPAViewer.class);
        LPAViewerJob.setInputFormatClass(TextInputFormat.class);
        LPAViewerJob.setMapperClass(LPAViewerMapper.class);
        LPAViewerJob.setMapOutputKeyClass(Text.class);
        LPAViewerJob.setMapOutputValueClass(Text.class);
        LPAViewerJob.setReducerClass(LPAViewerReducer.class);
        LPAViewerJob.setOutputKeyClass(Text.class);
        LPAViewerJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(LPAViewerJob, new Path(out + "labelFile_"+(i)));
        FileOutputFormat.setOutputPath(LPAViewerJob, new Path(out + "task5"));
        LPAViewerJob.waitForCompletion(true);
    }
}