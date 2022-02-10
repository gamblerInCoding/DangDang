

package org.example.backend.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//reduce的输入是map的输出 <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

//相同的key为一组...调用一次reduce方法，在方法内迭代这一组数据，进行计算：sum count max min


    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        //hello 1
        //hello 1
        //hello 1
        //hello 1
        //hello 1
        //hello 1

        // key:hello
        // values:(1,1,1,1)

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }


}



