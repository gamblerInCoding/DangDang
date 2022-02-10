package org.example.backend.mapreduce.finallyData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.example.backend.mapreduce.MyMapper;
import org.example.backend.mapreduce.TableReduce;

public class commentdatazhishujob {

    /*
     * 先写客户端
     */


    public void run(String kv) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS","hdfs://192.168.10.100:9000");
        conf.set("mapred.job.tracker","192.168.10.100");
        conf.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        conf.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir", "/mapreduce/hadoop");
        Job job = Job.getInstance(conf);
        job.setJarByClass(commentdatazhishujob.class);
        String jobname = "commentdatazhishujob"+kv;
        job.setJobName(jobname);
        String url2 = "/dataanalyse/crawler/MergePublish/book/";
        String url3 =  kv + "total.txt";
        Path input = new Path(url2+url3);
        FileInputFormat.addInputPath(job,input);
        String url4 = "\\PublishMerge\\month" + kv;
        String urx = "commentdatazhishujob"+kv;
        job.setMapperClass(finallyMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        TableMapReduceUtil.initTableReducerJob(urx, finallyReduce.class, job);
        job.waitForCompletion(true);



    }



}
