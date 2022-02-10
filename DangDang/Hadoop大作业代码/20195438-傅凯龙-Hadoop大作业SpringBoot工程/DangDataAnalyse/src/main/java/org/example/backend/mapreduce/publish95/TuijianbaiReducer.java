package org.example.backend.mapreduce.publish95;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

//reduce的输入是map的输出 <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class TuijianbaiReducer extends TableReducer<Text, IntWritable,Text> {
    private String family="1";
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        if(context!=null){
            String sk = new String(key.copyBytes());
            Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes("publisher"),Bytes.toBytes(sk));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes("number"),Bytes.toBytes(String.valueOf(sum)));
            context.write(key, put);
        }
    }
}

