package org.example.backend.mapreduce;

import org.example.backend.mapreduce.comment.CommentJob;
import org.example.backend.mapreduce.commentsum.Commentjobsum;
import org.example.backend.mapreduce.finallyData.commentdatazhishujob;
import org.example.backend.mapreduce.publish95.TuijianbaiJob;
import org.example.backend.mapreduce.tuijain.TuijianJob;
import org.example.backend.mapreduce.undertwo.Undertwojob;
import org.springframework.stereotype.Component;

@Component
public class CrawlerMapreduceDao {
    private static WordCount wordCount = new WordCount();
    private static CommentJob commentjob = new CommentJob();
    private static TuijianJob tuijianJob = new TuijianJob();
    private static TuijianbaiJob tuijianJobbai= new TuijianbaiJob();
    private static Undertwojob undertwojob= new Undertwojob();
    private static commentdatazhishujob fillll= new commentdatazhishujob();
    private static Commentjobsum js = new Commentjobsum();
    public void renwurun() throws Exception {

        for(int i=1;i<=9;i++)
        {
             String names = String.valueOf(i);
            wordCount.run(names);
            commentjob.run(names);
            tuijianJob.run(names);
            tuijianJobbai.run(names);
            undertwojob.run(names);
            fillll.run(names);
            js.run(names);

        }

    }

}
