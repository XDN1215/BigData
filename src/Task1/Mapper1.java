package Task1;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class Mapper1 extends Mapper<Object, Text, Text, NullWritable> {

    ArrayList<String> NameList = new ArrayList<>();// 原始的名字列表
    HashMap<String, String> Map = new HashMap<>();
    // Map映射，比如把Harry和Potter都映射到Harry Potter，而Harry Potter就映射到自己
    // 实现输出时的名字统一

    public void setup(Context context) throws IOException, InterruptedException {
        // 初始化时读入名字列表并建立映射

        Configuration config = context.getConfiguration();
        String nameFile = config.get("namePath");
        Path path = new Path(nameFile);

        FileSystem fileSystem = FileSystem.get(config);
        FSDataInputStream inputStream = fileSystem.open(path);
        LineReader lineReader = new LineReader(inputStream, config);
        Text line = new Text();// 打开名称列表文件

        while (lineReader.readLine(line) > 0) {
            String name = line.toString();
            NameList.add(name);
            //          context.write(new Text("***: " + name), NullWritable.get());
        } // 将所有名字加入列表中

        for (int i = 0; i < NameList.size(); i++) {
            String now = NameList.get(i);
            int n = now.split(" ").length;
            if (n > 1) Map.put(now, now);// 如果长度大于1，这就是人物全名
            else if (now.compareTo("Harry") == 0 || now.compareTo("Potter") == 0) Map.put(now, "Harry Potter");
            else if (now.compareTo("Ron") == 0 || now.compareTo("Weasley") == 0) Map.put(now, "Ron Weasley");
                // 否则寻找人物全名
                // 特判，哈利和罗恩

            else {
                String full_name = "";
                boolean flag = true;
                for (int j = 0; j < NameList.size(); j++) {
                    String[] tmp_list = NameList.get(j).split(" ");
                    int m = tmp_list.length;
                    if (m == 1) continue;// 与所有的人物全称比对，找到对应的全名
                    for (int k = 0; k < tmp_list.length; k++) {
                        if (tmp_list[k].compareTo(now) != 0) continue;// 与每一节比较
                        if (full_name.length() > 0) {
                            // 找到了不止一个全名，意味着简称冲突，该名字作废
                            flag = false;
                            break;
                        }
                        full_name = NameList.get(j);// 记录找到的全名
                        break;
                    }
                    if (flag == false) break;
                    // 作废，停止后续比对
                }
                if (flag) {
                    if (full_name.length() == 0) full_name = now;
                    //如果没找到全名又没有冲突，说明它本身就是全名
                    Map.put(now, full_name);// 将简称与全名的映射加入
                }

            }
        }
    }


    String compare(String[] team, int point) {
        int pos = point;
        for (int i = 0; i < NameList.size(); i++) {
            // 与列表里每一个名字比对
            pos = point;
            String[] now = NameList.get(i).split(" ");
            int k = 0;
            while (pos < team.length) {
                // 文本的第pos节与当前名字的第k节比对
                String tmp = team[pos];
                tmp = filter(tmp);
                //过滤掉多余符号
                if (tmp.compareTo(now[k]) != 0) break;

                pos++;
                k++;
                if (k == now.length) return NameList.get(i);
                // 到最后说明比对成功，返回该名字
            }
        }
        // 没有找到返回空串
        return "";
    }

    String filter(String s) {
        int n = s.length();
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (c >= 'a' && c <= 'z') continue;
            if (c >= 'A' && c <= 'Z') continue;
            if (c == '-') continue;
            return s.substring(0, i);
        }
        return s;
    }//为了去掉后面的符号，比如Harry. 或Ron's

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String name = ((FileSplit) context.getInputSplit()).getPath().getName();// 文档名
        if (name.compareTo("person_name_list.txt") == 0) return;// 如果该行是名称列表文件里的，就不处理，只处理正文

        String[] str = value.toString().split(" ");
        // 把每一行用空格分出若干节（单词）
        String list = "";// 记录这一行里出现的所有人名
        for (int i = 0; i < str.length; ) {
            String res = compare(str, i);
            // 以每一节为开头在名称列表里尝试匹配
            if (res.length() == 0) {
                i++;// 跳到下一节
                continue;// 没找到
            }
            //       context.write(new Text("find " + res), NullWritable.get());
            i += res.split(" ").length;// 跳到匹配的段后一个节
            if (!Map.containsKey(res)) continue;// 匹配了，但没有全名映射，说明因为冲突被舍弃
            res = Map.get(res);// 映射到全名
            list = list + res + ",";// 记录这一行出现了这个人名
        }
        if (list.length() > 0) list = list.substring(0, list.length() - 1);//去掉结尾多余的逗号
        context.write(new Text(list), NullWritable.get());//输出只保留人名的字符串

    }
}
