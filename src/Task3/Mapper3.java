package Task3;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper3 extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //<name1,name2> n------>name1 name2#n
        Text word1 = new Text();
        Text word2 = new Text();
        //去掉key两端的<>
        int tempLen = key.toString().length();
        String temp = key.toString().substring(1, tempLen-1);
		//将name1,name2,n分别提取出来
        String name1 = new String();
        String name2 = new String();
        String frequency = new String();
        name1 = temp.split(",")[0];
        name2 = temp.split(",")[1];
        frequency = value.toString();
        //将键值分别写入word1，word2
        word1.set(name1);
        word2.set(name2+"#"+frequency);
        context.write(word1, word2);
    }
}
