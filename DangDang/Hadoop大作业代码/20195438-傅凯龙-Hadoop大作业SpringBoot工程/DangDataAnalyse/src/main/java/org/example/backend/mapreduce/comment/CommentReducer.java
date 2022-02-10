package org.example.backend.mapreduce.comment;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.example.backend.entity.CommentBean;
import org.example.backend.entity.FlowBean;

import java.io.IOException;

public class CommentReducer  extends TableReducer<Text, CommentBean,Text> {
    private String family="1";
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<CommentBean> values,
                       Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (CommentBean val : values) {
            if(context!=null){
                String sk = new String(key.copyBytes());
                Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes("name"),Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes("comment"),Bytes.toBytes(String.valueOf(val.getComment())));
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes("price"),Bytes.toBytes(String.valueOf(val.getPrice())));
                context.write(key, put);
            }
        }
    }
}
