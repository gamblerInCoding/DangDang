package org.example.backend.mapreduce.jichu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

//Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
/*
 * map数据类型不使用基本数据类型
 *
 * 要用hadoop的数据类型
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //key放置字符串的偏移量(行的偏移量)//value  放的是一行数据	     Object -> LongWritable
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        StringTokenizer itr = new StringTokenizer(line.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }

}