package org.example.backend.mapreduce.publish95;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
/*
 * map数据类型不使用基本数据类型
 *
 * 要用hadoop的数据类型
 */
public class TuijianbaiMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //key放置字符串的偏移量(行的偏移量)//value  放的是一行数据	     Object -> LongWritable
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String []files = line.split("   ");
        if((files.length-4)==-3){

        }
        else {
            double ceshi = Double.parseDouble(files[files.length - 4]);
            if (ceshi >= 95.0) {
                String names = files[files.length - 2];
                word = new Text(names);
                context.write(word, one);
            }
        }
    }
}