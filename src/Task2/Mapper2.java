package Task2;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] itr=value.toString().split(",");//将输入的一行按逗号划分为数组，数组中每一项是一个名字
        for(int i=0;i<itr.length;++i)//两层循环，遍历数组中任意两个元素组合的情况
            for(int j=0;j<itr.length;++j)
                if(!itr[i].equals(itr[j]))//如果两个元素不相同，则同现一次
                    context.write(new Text("<"+itr[i]+","+itr[j]+">"),new IntWritable(1));
    }
}
