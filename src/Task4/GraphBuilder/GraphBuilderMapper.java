package Task4.GraphBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // value形如 Harry Potter[Ron,0.66667|Hermione,0.33333]
        Configuration config = context.getConfiguration();
        String init_score = config.get("init_score");// 获取初始值

        String[] value_list = value.toString().split("\\[");
        String key_out = value_list[0];// Harry Potter
        String value_out = value_list[1].substring(0, value_list[1].length() - 1);
        // Ron,0.66667|Hermione,0.33333

        value_out = init_score + ':' + value_out;
        context.write(new Text(key_out), new Text(value_out));
        // 输出形如Harry Potter 0.2:Ron,0.66667|Hermione,0.33333
    }
}
