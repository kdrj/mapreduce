package main.java.com.jouryu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @program: WordCount
 * @description: 入口类
 * @author: kdrj
 * @date: 2018-11-15 09:05
 **/
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        String[] otherOrgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherOrgs.length<2){
            System.err.println("必须输入文件读取路径和输出路径");
            System.exit(2);
        }
        Job job=new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount app");

        FileInputFormat.addInputPath(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
