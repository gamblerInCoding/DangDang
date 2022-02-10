package org.example.backend.crawler;

import org.example.backend.hbase.CrawlerHBaseDao;
import org.example.backend.hdfs.CrawlerHdfsDao;
import org.example.backend.mapreduce.CrawlerMapreduceDao;
import org.example.backend.mapreduce.MapReduce_publish;
import org.example.backend.mapreduce.WordCount;
import org.example.backend.utils.CallShell;
import org.springframework.stereotype.Component;
import org.example.backend.utils.ReadHbaseDataToHDFS;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

@Component
public class LogCollection extends TimerTask{

    private static String url2 = System.getProperty("user.dir");
    private static String url0 = "//src//main//java//org//example//backend//crawler//pachong";
    private static String shell_path = "E:\\pachong.py";
    private static CrawlerHdfsDao crawlerHdfsDao = new CrawlerHdfsDao();
    private static CrawlerHBaseDao crawlerHBaseDao;
    private static CrawlerMapreduceDao crawlerMapreduceDao = new CrawlerMapreduceDao();
    private static ReadHbaseDataToHDFS readHbaseDataToHDFS = new ReadHbaseDataToHDFS();
    private static WordCount wordCount = new WordCount();
    static {
        try {
            crawlerHBaseDao = new CrawlerHBaseDao();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void run() {
        // TODO Auto-generated method stub
        long startTime = System.currentTimeMillis();
        System.out.println("-----------开始采集--------");
        Date day=new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(day));
        try {
            String[] str = {"python",shell_path};
            String resultMsg = CallShell.call(str);
            System.out.println(resultMsg);
            crawlerHdfsDao.filePut();
            crawlerHdfsDao.exceltotxt();
            crawlerHdfsDao.exceltotxts();
            crawlerHBaseDao.updateData();
            crawlerHBaseDao.updateResult();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(int i=1;i<=9;i++)
        {
            String tablenames = "publish"+String.valueOf(i);
            try {
                crawlerHBaseDao.deleteTable(tablenames);
                crawlerHBaseDao.createTableTest(i);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            crawlerMapreduceDao.renwurun();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("-----------采集结束--------");
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }

}
