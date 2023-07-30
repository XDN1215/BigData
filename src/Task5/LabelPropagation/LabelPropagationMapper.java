package Task5.LabelPropagation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

public class LabelPropagationMapper extends Mapper<Object, Text, IntWritable, Text> {
    Map<String, String> labelMap = new HashMap<>();
    Map<String, List<String>> neighbourMap = new HashMap<>();
    boolean isLPAOver = true;

    @Override
    protected void setup(Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fileSystem = FileSystem.get(conf);
        //读取labelLocalFile文件，建立labelMap
        String labelLocalFile = conf.get("out")+"labelLocalFile";
        Path labelLocalFilePath = new Path(labelLocalFile);
        FSDataInputStream labelFsDataInputStream = fileSystem.open(labelLocalFilePath);
        String labelLine = labelFsDataInputStream.readLine();
        while(labelLine!=null){
            String[] splits = labelLine.split(",", 2);
            labelMap.put(splits[0], splits[1]);
            labelLine = labelFsDataInputStream.readLine();
        }
        labelFsDataInputStream.close();
        fileSystem.delete(labelLocalFilePath, true);
        //读取neighbourLocalFile文件，建立neighbourMap
        String neighbourLocalFile = conf.get("out")+"neighbourLocalFile";
        Path neighbourLocalFilePath = new Path(neighbourLocalFile);
        FSDataInputStream neighbourFsDataInputStream = fileSystem.open(neighbourLocalFilePath);
        String neighbourLine = neighbourFsDataInputStream.readLine();
        while(neighbourLine!=null){
            String[] splits = neighbourLine.split("\\[", 2);
            List<String> neighbourList = new ArrayList<>();
            String temp = splits[1];
            int tempLen = temp.length();
            String[] neighbours = temp.substring(0, tempLen-1).split("\\|");
            neighbourList.addAll(Arrays.asList(neighbours));
            neighbourMap.put(splits[0], neighbourList);
            neighbourLine = neighbourFsDataInputStream.readLine();
        }
        neighbourFsDataInputStream.close();
    }

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        IntWritable word1 = new IntWritable();
        Text word2 = new Text();
        String name = new String();
        String label = new String();
        //从value中提取name以及对应的label
        name = value.toString().split(",")[0];
        label = value.toString().split(",")[1];

        //以一定的概率选择更新name的标签，通过生成随机数判断随机数是否大于0.8实现，若随机数>0.8，则不计算新标签，仍然输出旧标签
        double r = new Random().nextDouble();
        if(r>0.8){
            Random random2 = new Random();
            int randX2 = random2.nextInt();

            word1.set(randX2);
            word2.set(name+","+label);
            context.write(word1, word2);
            return;
        }

        //根据setup建立的labelMap和neighbourMap，计算name的所有邻居的标签中权重最大的一项标签,即name的新标签
        Map<String, Double> counts = new HashMap<>();
        String labelMaxmum = new String("");//权重最大的标签
        Double countMax = 0.0;
        if(neighbourMap.containsKey(name)){
            for(String neighbouri: neighbourMap.get(name)){//遍历具有人物图中name的邻居节点
                String neighbouriName = neighbouri.split(",")[0];
                String neighbouriProb = neighbouri.split(",")[1];
                if(labelMap.containsKey(neighbouriName)){
                    String labeli = labelMap.get(neighbouriName);
                    if(counts.containsKey(labeli)){
                        Double prev = counts.get(labeli);
                        counts.put(labeli, prev+Double.parseDouble(neighbouriProb));//标签的权重为共现概率之和
                    }
                    else counts.put(labeli, Double.parseDouble(neighbouriProb));//标签的权重为共现概率之和
                    if(counts.get(labeli)>countMax){
                        countMax=counts.get(labeli);
                        labelMaxmum = labeli;
                    }
                    else if(counts.get(labeli)==countMax){//当有多个最大权重的标签时，随机选择一个标签
                        Random random1 = new Random();
                        int randX1 = random1.nextInt();
                        if(randX1%2==0){//通过生成随机数、判断随机数是否是2的倍数，实现随机选择
                            countMax=counts.get(labeli);
                            labelMaxmum = labeli;
                        }
                    }
                }
            }
        }
        //比较节点的新旧标签不一致，说明迭代仍需继续，标记isLPAOver应该被赋值为false
        if(!labelMaxmum.equals(label)&&!labelMaxmum.equals("")){
            isLPAOver = false;
        }
        //生成随机数作为key输出，利用MapReduce的自排序将所有数据项的顺序打乱
        Random random = new Random();
        int randX = random.nextInt();
        //将key、value写入中间结果
        word1.set(randX);
        word2.set(name+","+labelMaxmum);
        context.write(word1, word2);
    }

    @Override
    protected void cleanup(Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        //将标记isLPAOver的值写入本地文件isLPAOverFile，在迭代的过程中可以通过读取此文件的内容判断循环要不要继续
        Configuration conf = context.getConfiguration();
        String isLPAOverFilePath = conf.get("isLPAOverFile");
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(isLPAOverFilePath));
        fsDataOutputStream.write(String.valueOf(isLPAOver).getBytes());
        fsDataOutputStream.close();
    }
}
