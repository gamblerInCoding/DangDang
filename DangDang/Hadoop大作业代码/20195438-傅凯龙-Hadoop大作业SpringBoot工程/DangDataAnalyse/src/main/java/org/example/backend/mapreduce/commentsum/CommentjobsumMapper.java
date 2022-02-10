package org.example.backend.mapreduce.commentsum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.backend.entity.CommentBean;

import java.io.IOException;

public class CommentjobsumMapper extends Mapper<Object, Text, Text, IntWritable> {
    private CommentBean one = new CommentBean();
    private Text word = new Text();
    //key放置字符串的偏移量(行的偏移量)//value  放的是一行数据	     Object -> LongWritable
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String []files = line.split("   ");
        if((files.length - 5)==-4){}else {
            int up = Integer.parseInt(files[files.length - 5]);
            String names = files[files.length - 2];
            IntWritable one = new IntWritable(up);
            word = new Text(names);
            context.write(word, one);
        }

    }
}