package org.example.backend.mapreduce.undertwo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.backend.entity.BookItems2;
import org.example.backend.entity.CommentBean;
import org.example.backend.entity.FlowBean;

import java.io.IOException;

public class UndertwoMapper extends Mapper<Object, Text, Text, FlowBean> {
    private FlowBean one = new FlowBean();
    private Text word = new Text();
    //key放置字符串的偏移量(行的偏移量)//value  放的是一行数据	     Object -> LongWritable
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        System.out.println("!!!!!"+line);
        String []files = line.split("   ");
        String money = files[files.length - 1].replace(",","");
        System.out.println("++++"+money);
        if(money.equals("")){

        }else {
            double low = Double.parseDouble(money);
            double up = Double.parseDouble(files[files.length - 5]);
            String names = files[files.length - 4];
            one.setPrice(up);
            one.setTuijian(low);
            word = new Text(names);
            context.write(word, one);
        }


    }
}