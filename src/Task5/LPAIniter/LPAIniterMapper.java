package Task5.LPAIniter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class LPAIniterMapper extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String name = value.toString().split("\\[")[0];
        String label = name;
        Random random = new Random();
        int random_number = random.nextInt();
        context.write(new IntWritable(random_number), new Text(name + ',' + label));
    }
}

