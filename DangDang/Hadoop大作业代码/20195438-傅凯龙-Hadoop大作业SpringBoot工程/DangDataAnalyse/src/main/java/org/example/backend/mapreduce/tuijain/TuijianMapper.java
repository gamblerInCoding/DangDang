package org.example.backend.mapreduce.tuijain;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.example.backend.entity.FlowBean;

import java.io.IOException;

//Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
/*
 * map数据类型不使用基本数据类型
 *
 * 要用hadoop的数据类型
 */
public class TuijianMapper extends Mapper<Object, Text, Text, FlowBean> {
    private FlowBean one = new FlowBean();
    private Text word = new Text();
    //key放置字符串的偏移量(行的偏移量)//value  放的是一行数据	     Object -> LongWritable
    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String []files = line.split("   ");
        if((files.length-6)==-5){

        }else {
            String name = files[files.length - 6];
            long phone = Long.parseLong(files[0]);
            double low = Double.parseDouble(files[files.length - 4]);
            if (low == 100.0) {
                String money = files[files.length - 1].replace(",","");
                double up = Double.parseDouble(money);
                one = new FlowBean(low, up);
                String ceshi = String.valueOf(phone);
                word = new Text(name);
                context.write(word, one);
            }
        }
    }
}