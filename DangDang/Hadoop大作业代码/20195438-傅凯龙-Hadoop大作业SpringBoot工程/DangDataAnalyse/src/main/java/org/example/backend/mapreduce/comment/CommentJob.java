package org.example.backend.mapreduce.comment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.example.backend.entity.CommentBean;
import org.example.backend.entity.FlowBean;
import org.example.backend.mapreduce.MyMapper;
import org.example.backend.mapreduce.TableReduce;
import org.example.backend.mapreduce.WordCount;
import org.example.backend.mapreduce.tuijain.TuijianJob;
import org.example.backend.mapreduce.tuijain.TuijianMapper;
import org.example.backend.mapreduce.tuijain.TuijianReducer;

public class CommentJob {
    public void run(String kv) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS","hdfs://192.168.10.100:9000");
        conf.set("mapred.job.tracker","192.168.10.100");
        conf.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        conf.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        System.setProperty("hadoop.home.dir", "/mapreduce/hadoop");
        Job job = Job.getInstance(conf);
        job.setJarByClass(CommentJob.class);
        String jobname = "Commentjob"+kv;
        job.setJobName(jobname);
        String url2 = "/dataanalyse/crawler/MergePublish/book/";
        String url3 =  kv + "total.txt";
        Path input = new Path(url2+url3);
        FileInputFormat.addInputPath(job,input);
        String urx = "comment"+kv;
        job.setMapperClass(CommentMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CommentBean.class);
        TableMapReduceUtil.initTableReducerJob(urx, CommentReducer.class, job);
        job.waitForCompletion(true);



    }

}
