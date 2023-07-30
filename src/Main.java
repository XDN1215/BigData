import java.io.IOException;

//import Driver.DriverMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public class Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2 && otherArgs.length != 3) {
            System.err.println("Usage: Lab5 <in> <out>");
            System.exit(2);
        }
        String in = "";
        String out = "";
        if (otherArgs.length == 3) {
            out = args[2];
            in = args[1];
        } else {
            out = args[1];
            in = args[0];
        }
        if (out.charAt(out.length() - 1) != '/') out = out + "/";
        String namePath = "/data/2023s/exp2/person_name_list.txt";
        //task1
        conf.set("namePath", namePath);
        Job job1 = Job.getInstance(conf, "Task1");
        job1.setJarByClass(Main.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(Task1.Mapper1.class);
        job1.setReducerClass(Task1.Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job1, new Path(in));
        FileOutputFormat.setOutputPath(job1, new Path(out + "task1"));
        job1.waitForCompletion(true);
        //task2
        Job job2 = Job.getInstance(conf, "Task2");
        job2.setJarByClass(Main.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(Task2.Mapper2.class);
        job2.setReducerClass(Task2.Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(out + "task1"));
        FileOutputFormat.setOutputPath(job2, new Path(out + "task2"));
        job2.waitForCompletion(true);
        //task3
        //debug
        conf.set("out", out);

        Job job3 = Job.getInstance(conf, "Task3");
        job3.setJarByClass(Main.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(Task3.Mapper3.class);
        job3.setReducerClass(Task3.Reducer3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(out + "task2"));
        FileOutputFormat.setOutputPath(job3, new Path(out + "task3"));
        job3.waitForCompletion(true);

        //task4
        String[] param = {out + "task3", out + "task4"};
        Task4.Driver.DriverMain.main(param);
        //task5


        Task5.LPAIniter.LPAIniter.LPAIniterMain(out);//得到最初的标签文件
        String isLPAOver = "false";//初始设置为false，以进入第一次循环
        int i = 0;
        while (isLPAOver.equals("false")&&i<10) {//i<10设置了一个迭代上界，避免死循环
            Task5.LabelPropagation.LabelPropagation.LabelPropagationMain(out, i);//更新标签文件和isLAPOverFile文件
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inStream = fs.open(new Path(out + "isLPAOverFile"));
            isLPAOver = inStream.readLine();
            inStream.close();
            i++;
        }
        Task5.LPAViewer.LPAViewer.LPAViewerMain(out, i);//整理最后一次迭代后的结果，得到以标签为键的输出
        System.exit(0);
    }
}
