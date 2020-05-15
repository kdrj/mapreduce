package main.java.com.jouryu.mapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @program: WordCount
 * @description: wordCountReduce
 * @author: kdrj
 * @date: 2018-11-15 08:55
 **/
public class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int count=0;
        for (IntWritable value:values) {
            count++;
        }
        context.write(key,new IntWritable(count));
    }
}
