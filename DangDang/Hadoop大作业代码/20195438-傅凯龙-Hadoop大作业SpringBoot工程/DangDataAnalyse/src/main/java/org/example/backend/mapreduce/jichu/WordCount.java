package org.example.backend.mapreduce.jichu;

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

public class WordCount {

    /*
     * 先写客户端
     */


    public void run(String kv) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        conf.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        String jobname = "WordConut"+kv;
        job.setJobName(jobname);
        String url2 = System.getProperty("user.dir");
        String url3 = "\\book" + kv + ".txt";
        Path input = new Path(url2+url3);
        FileInputFormat.addInputPath(job,input);
        String url4 = "\\PublishMerge\\month" + kv;
        String urx = "publish"+kv;
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        TableMapReduceUtil.initTableReducerJob(urx, TableReduce.class, job);
        job.waitForCompletion(true);
    }



}
