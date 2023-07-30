package Task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Reducer3 extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        Text word1 = new Text();

        int sum = 0;
        StringBuilder posting = new StringBuilder();
        posting.append("[");
        //第一次遍历，求得共现次数总和
        Iterator<Text> iterator1 = values.iterator();
        ArrayList<String> stringList = new ArrayList<>();
        while (iterator1.hasNext()) {
            String s=iterator1.next().toString();
            stringList.add(s);
            String[] temp = s.split("#");
            int count = Integer.parseInt(temp[1]);
            sum += count;
        }
        //第二次遍历，用单个共现次数/共现次数总和得到共现概率，并将结果追加到posting
        for(String s:stringList)
        {
            String[] temp = s.split("#");
            int count = Integer.parseInt(temp[1]);
            float probability = ((float) count)/((float) sum);
            String prob = String.valueOf(probability);
            posting.append(temp[0]+","+prob);
            posting.append("|");
        }
        int postingLen = posting.length();
        posting.deleteCharAt(postingLen-1);
        posting.append("]");
        //将输出key、value写入结果
        word1.set(key+posting.toString());
        context.write(word1, NullWritable.get());

        //将输出key、value写入本地的neighbourLocalFile，方便task5的setup()读取
        Configuration conf = context.getConfiguration();
        FileSystem fileSystem = FileSystem.get(conf);
        String neighbourLocalFile = conf.get("out")+"neighbourLocalFile";
        FSDataOutputStream fsDataOutputStream = null;
        if (!fileSystem.exists(new Path(neighbourLocalFile))) {
            fsDataOutputStream = fileSystem.create(new Path(neighbourLocalFile));
        }else{
            fsDataOutputStream = fileSystem.append(new Path(neighbourLocalFile));
        }
        fsDataOutputStream.write(new String(word1.toString()+"\n").getBytes());
        fsDataOutputStream.close();
    }
}
