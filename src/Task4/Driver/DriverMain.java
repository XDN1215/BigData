package Task4.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class DriverMain {

    public static Integer times = 5;

    // 迭代次数
    public static Double count(String s) throws IOException {
        Configuration config = new Configuration();
        FileSystem fileSystem = FileSystem.get(config);
        FSDataInputStream inputStream = fileSystem.open(new Path(s));
        LineReader lineReader = new LineReader(inputStream, config);
        // 打开Task3的输出文件

        Text line = new Text();
        Integer num = 0;

        while (lineReader.readLine(line) > 0) {
            num++;
        } // 每行形如Hermione [Harry, 0.33333 | Ron, 0.66667]
        // 每读到一行，说明存在一个点

        fileSystem.close();

        if (num == 0) {
            return -5.0;
        }
        return 1.0 / num;
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        String out = args[1];
        String in = args[0];

        if (out.charAt(out.length() - 1) != '/')
            out = out + "/";
        // in，out分别是输入输出文件

        Double init_score = count(in + "/part-r-00000");
        // 计算pagerank初始值，保存在init_score中

        String[] param = {in, out + "round_0"};
        Task4.GraphBuilder.GraphBuilderMain.main(param, init_score);
        // 首先建图，最开始的输出是Iter第0次的输入

        for (int i = 0; i < times; i++) {
            param[0] = out + "round_" + (i);
            param[1] = out + "round_" + (i + 1);
            Task4.Iter.IterMain.main(param, init_score);
        }
        // 进行times次迭代求值

        param[0] = out + "round_" + (times);
        param[1] = out + "over";
        Task4.Viewer.ViewerMain.main(param, init_score);
        // 最后输出结果到out
    }
}
