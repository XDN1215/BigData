package Task4.Iter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IterMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // value形如 Harry Potter!0.2:Ron,0.66667|Hermione,0.33333
        String[] value_list = value.toString().split("!");
        Double score = Double.parseDouble(value_list[1].split(":")[0]);
        String edge = value_list[1].split(":")[1];

        // 1.传递边集
        context.write(new Text(value_list[0]), new Text(edge));

        // 2.传递该点给周围点生成的分数
        String[] edge_list = edge.split("\\|");

        for (int i = 0; i < edge_list.length; i++) {
            String[] pair = edge_list[i].split(",");
            // 形如Ron,0.66667
            String name = pair[0];// 获取人名
            Double point = Double.parseDouble(pair[1]) * score;
            // 计算它得到的分数
            context.write(new Text(name), new Text(point.toString()));
            // 输出形如Ron 0.13334
        }
    }
}
