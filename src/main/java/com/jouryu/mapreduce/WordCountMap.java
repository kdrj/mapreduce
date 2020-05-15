package main.java.com.jouryu.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: WordCount
 * @description: 词频统计
 * @author: kdrj
 * @date: 2018-11-15 08:45
 **/
public class WordCountMap extends Mapper<LongWritable,Text,Text,IntWritable> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line=value.toString();

        String str[]=line.split("");
        for (String s:str) {
            context.write(new Text(s),new IntWritable(1));
        }

    }
}
