package org.example.backend.mapreduce.finallyData;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class finallyReduce  extends TableReducer<Text, IntWritable,Text> {
    private String family="1";
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            if(context!=null){
                String sk = new String(key.copyBytes());
                Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes("comment"),Bytes.toBytes(String.valueOf(val.get())));
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes("tuijian"),Bytes.toBytes(String.valueOf(key.toString())));
                context.write(key, put);
            }
        }

    }
}
