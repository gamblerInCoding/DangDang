package org.example.backend.mapreduce.publish95;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TuijianbaiJob {
    public void run(String kv) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS","hdfs://192.168.10.100:9000");
        conf.set("mapred.job.tracker","192.168.10.100");
        conf.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        conf.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir", "/mapreduce/hadoop");
        Job job = Job.getInstance(conf);
        job.setJarByClass(TuijianbaiJob.class);
        String jobname = "Tuijianbaijob"+kv;
        job.setJobName(jobname);
        String url2 = "/dataanalyse/crawler/MergePublish/book/";
        String url3 =  kv + "total.txt";
        Path input = new Path(url2+url3);
        FileInputFormat.addInputPath(job,input);
        String urx = "baituijian"+kv;
        job.setMapperClass(TuijianbaiMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        TableMapReduceUtil.initTableReducerJob(urx, TuijianbaiReducer.class, job);
        job.waitForCompletion(true);



    }

}
