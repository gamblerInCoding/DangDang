package org.example.backend.mapreduce.tuijain;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.example.backend.entity.FlowBean;

import java.io.IOException;

//reduce的输入是map的输出 <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class TuijianReducer  extends TableReducer<Text, FlowBean,Text> {
    private String family="1";
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<FlowBean> values,
                       Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (FlowBean val : values) {

            if(context!=null){
                String sk = new String(key.copyBytes());
                Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes("title"),Bytes.toBytes(key.toString()));
                System.out.println(key.toString());
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes("price"),Bytes.toBytes(String.valueOf(val.getPrice())));
                System.out.println("***********2");
                context.write(key, put);
            }
        }
    }
}

