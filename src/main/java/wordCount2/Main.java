package wordCount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;

/**
 * Created by sghipr on 15-12-18.
 */
public class Main {

    public static class wordMap extends Mapper<LongWritable,Text,Text,IntWritable> {

        private IntWritable count = new IntWritable(1);

        public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{

            String[] array = value.toString().split(",", -1);
            for(String str : array){
                context.write(new Text(str),count);
            }
        }
    }
    /**
     *
     */
    public static class wordReduce  extends Reducer<Text,IntWritable,Text,IntWritable> {

        private MultipleOutputs outputs = null;

        private IntWritable sum = new IntWritable(0);

        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{

            int count = 0;
            for(IntWritable value : values)
                count += value.get();
            sum.set(count);
            context.write(key,sum);
        }
    }

    /**
     * wordCount 运行时的主程序,本质就是在配置文件.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws  Exception{

        Configuration conf = new Configuration();

        Job job = new Job(conf,"Main");
        job.setJarByClass(Main.class);

        //注意在本地运行与在集群上运行时，文件路径的问题.
        //在集群上运行,一般来说,输入与输出的路径都是在hdfs,即文件系统上. hadoop jar wordCount.jar
        String inFile = "/usr/local/input/*";
        String outFile = "/home/hadoop/output";//在集群上相当于:hdfs://localhost:9000/tmp/output


        FileSystem.get(URI.create(inFile),conf);//设置为本地文件系统.

        Path inPath = new Path(inFile);
        Path outPath = new Path(outFile);

        //设置
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Main.wordMap.class);
        job.setReducerClass(Main.wordReduce.class);

        //动态设置reduceTask的个数.
        job.setNumReduceTasks(3);

        //注意，这里的设置是针对的map
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出文件存在的话，则将之删除.
        FileSystem.get(URI.create(inFile),conf).delete(outPath,true);

        //设置输入输出文件路径.
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);

//        Set<String> set = new TreeSet<String>(new Comparator<String>() {
//            public int compare(String o1, String o2) {
//                return 0;
//            }
//        })



        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
