package org.example.backend.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import static org.apache.hadoop.hbase.KeyValue.*;

public class MapReduce_publish {
    public void run() throws IOException {
        String tableName = "book1month";
        String family = "1";
        String column = "publisher";
        String targetTbale = "mergerPublish";
        System.out.println("tableName=" + tableName + ", family=" + family + ", column=" + column + ", targetTbale="
                + targetTbale);

        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS","hdfs://hadoop/");
        conf.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        conf.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        try {
            Job job = Job.getInstance(conf, "analyze table data for " + tableName);
            job.setJarByClass(MapReduce_publish.class);
            //Bytes.toBytes(tableName)
            TableMapReduceUtil.initTableMapperJob(tableName.getBytes(), scan, TableAnalyzeMap.class, Text.class,
                    IntWritable.class,job,false);
            TableMapReduceUtil.initTableReducerJob(targetTbale, TableAnalyzeReduce.class, job);
            job.setMapperClass(TableAnalyzeMap.class);
            job.setReducerClass(TableAnalyzeReduce.class);
            job.setCombinerClass(TableAnalyzeCombin.class);
            job.setNumReduceTasks(1);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
//        String table = "book1month";
//        String column = "publisher";
//        String outPath = "hdfs://dangdangoutput/mrReadHBase";
//
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
//        conf.set("hbase.zookeeper.property.clientPort","2181"); //端口号
//        conf.set(TableInputFormat.INPUT_TABLE, "book1month");
//        Scan scan = new Scan();
//        scan.setCaching(500);
//        scan.setCacheBlocks(false);
//        scan.addColumn(Bytes.toBytes("1"), Bytes.toBytes("publisher"));
//        try {
//            Job job = Job.getInstance(conf, "MergePublisher" + "book1month");
//            job.setJarByClass(MapReduce_publish.class);
//            job.setMapperClass(TableAnalyzeMap.class);
//            job.setInputFormatClass(TableInputFormat.class);
//            TableInputFormat.addColumns(scan, KeyValue.parseColumn(Bytes.toBytes("publisher")));
//
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(IntWritable.class);
//
//            TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("book1month"), scan, TableAnalyzeMap.class, Text.class,
//                    IntWritable.class, job);
//            TableMapReduceUtil.initTableReducerJob("total1publisher", TableAnalyzeReduce.class, job);
//            job.setMapperClass(TableAnalyzeMap.class);
//            job.setReducerClass(TableAnalyzeReduce.class);
//            job.setCombinerClass(TableAnalyzeCombin.class);
//            job.setNumReduceTasks(1);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
