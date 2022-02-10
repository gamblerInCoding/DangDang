package org.example.backend.mapreduce.undertwo;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.example.backend.entity.BookItems2;
import org.example.backend.entity.FlowBean;

import java.io.IOException;

public class UndertwoReduce  extends TableReducer<Text, FlowBean,Text> {
    private String family="1";
    private FlowBean result = new FlowBean();
    public void reduce(Text key, Iterable<FlowBean> values,
                       Context context) throws IOException, InterruptedException {

for(FlowBean vvs:values)
        if(vvs.getPrice()>=100000&&vvs.getTuijian()<=20){
            String sk = new String(key.copyBytes());
            Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes("comment"),Bytes.toBytes(String.valueOf(vvs.getPrice())));
            put.addColumn(Bytes.toBytes(family),Bytes.toBytes("tuijian"),Bytes.toBytes(String.valueOf(vvs.getTuijian())));
            context.write(key, put);
        }


    }
}
