package Task4.Viewer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;

public class ViewerMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // value形如 Harry!0.2:Ron,0.66667|Hermione,0.33333
        String value_list = value.toString();
        String name = value_list.split("!")[0];// 获取名字
        String score = value_list.split("!")[1].split(":")[0];
        // 获取分数
        context.write(new DoubleWritable(-Double.parseDouble(score)), new Text(name));
        // 因为要利用排序，所以将分数作为key输出，要正序所以分数大的在后面
    }
}