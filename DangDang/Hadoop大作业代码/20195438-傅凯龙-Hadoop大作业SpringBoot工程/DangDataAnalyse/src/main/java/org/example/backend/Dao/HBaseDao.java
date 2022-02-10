package org.example.backend.Dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.example.backend.entity.*;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
@Repository
public class HBaseDao {
    private static HBaseAdmin admin = null;
    // 定义配置对象HBaseConfiguration
    private static Configuration configuration;
    private static Connection connection;
    public static void init() throws IOException {
        if (connection!=null&&admin!=null){
            return;
        }
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        connection = ConnectionFactory.createConnection(configuration);
        try {
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void close(){
        try{
            if (admin!=null){
                admin.close();
                admin=null;
            }
            if (connection!=null){
                connection.close();
                connection=null;
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    public List<BookItem> scanTable()throws IOException{
        String tablename = "book1month";
        List<BookItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            BookItem bs = new BookItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ranking")){
                    bs.setRanking(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("author")){
                    bs.setAuthor(Bytes.toString(CellUtil.cloneValue(cell)));
                }

                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                    bs.setTitle(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("recommend")){
                    bs.setRecommend(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("prices")){
                    bs.setPrices(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        close();
        return books;
    }
    public List<ArrayList> lookAllTheprice()throws IOException{
        List<ArrayList> model = new ArrayList<>();
        for(int i=1;i<=9;i++) {
            ArrayList<Double> models = new ArrayList<>();
            String title = Integer.toString(i);
            System.out.println(title);
            String tablename = "book" + title + "month";
            System.out.println(tablename);
            List<BookItem> books = new ArrayList<>();
            //1.获取表对象
            System.out.println(tablename);
            Table table = connection.getTable(TableName.valueOf(tablename));
            //2.构建Scan对象
            Scan scan = new Scan();


            //3.扫描表
            ResultScanner resultScanner = table.getScanner(scan);

            //4.解析result
            for (Result result : resultScanner) {
                BookItem bs = new BookItem();
                //5.解析result
                String pads = "";
                for (Cell cell : result.rawCells()) {

                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("prices")) {
                        pads = Bytes.toString(CellUtil.cloneValue(cell));
                    }
                }
                String deleteString = pads.replace(",","");
                double kkk = Double.parseDouble(deleteString);
                models.add(kkk);
            }
            model.add(models);
            table.close();
        }
        close();
        return model;
    }
    public List<ArrayList> lookAllprice()throws IOException{
        List<ArrayList> model = new ArrayList<>();
        for(int i=1;i<=1;i++) {
            ArrayList<Double> models = new ArrayList<>();
            String title = Integer.toString(i);
            System.out.println(title);
            String tablename = "book" + title + "month";
            System.out.println(tablename);
            List<BookItem> books = new ArrayList<>();
            //1.获取表对象
            System.out.println(TableName.valueOf(tablename));
            Table table = connection.getTable(TableName.valueOf(tablename));

            //2.构建Scan对象
            Scan scan = new Scan();


            //3.扫描表
            ResultScanner resultScanner = table.getScanner(scan);

            //4.解析result
            for (Result result : resultScanner) {
                BookItem bs = new BookItem();
                //5.解析result
                String pads = "";
                for (Cell cell : result.rawCells()) {

                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("prices")) {
                        pads = Bytes.toString(CellUtil.cloneValue(cell));
                    }
                }
                String deleteString = pads.replace(",","");
                double kkk = Double.parseDouble(deleteString);
                models.add(kkk);
            }
             model.add(models);
            table.close();
        }
        close();
        return model;
    }
    public List<ArrayList> looksearchprice(@RequestBody Map<String,Object> map)throws IOException{
        String titlse = ((String)map.get("searchText"));
        int mos = Integer.parseInt(titlse);
        List<ArrayList> model = new ArrayList<>();

            ArrayList<Double> models = new ArrayList<>();
            String tablename = "book" + titlse + "month";
            System.out.println(tablename);
            List<BookItem> books = new ArrayList<>();
            //1.获取表对象
            System.out.println(TableName.valueOf(tablename));
            Table table = connection.getTable(TableName.valueOf(tablename));

            //2.构建Scan对象
            Scan scan = new Scan();


            //3.扫描表
            ResultScanner resultScanner = table.getScanner(scan);

            //4.解析result
            for (Result result : resultScanner) {
                BookItem bs = new BookItem();
                //5.解析result
                String pads = "";
                for (Cell cell : result.rawCells()) {

                    if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals("prices")) {
                        pads = Bytes.toString(CellUtil.cloneValue(cell));
                    }
                }
                String deleteString = pads.replace(",","");
                double kkk = Double.parseDouble(deleteString);
                models.add(kkk);
            }
            model.add(models);
            table.close();

        close();
        return model;
    }
    public List<BookItem> lookscanTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "book"+title+"month";
        List<BookItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            BookItem bs = new BookItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ranking")){
                    bs.setRanking(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("author")){
                    bs.setAuthor(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                    bs.setTitle(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("recommend")){
                    bs.setRecommend(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("prices")){
                    bs.setPrices(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        close();
        return books;
    }

    public List<BookItem> sclnTable()throws IOException{
        String tablename = "book9month";
        List<BookItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            BookItem bs = new BookItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ranking")){
                    bs.setRanking(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("author")){
                    bs.setAuthor(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                    bs.setTitle(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("recommend")){
                    bs.setRecommend(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("prices")){
                    bs.setPrices(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        close();
        return books;
    }
    public List<BookItem> looksclnTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        if(title.equals("24"))title="10";
        else if(title.equals("7"))title="11";
        else title="12";
        String tablename = "book"+title+"month";
        List<BookItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            BookItem bs = new BookItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ranking")){
                    bs.setRanking(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("author")){
                    bs.setAuthor(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                    bs.setTitle(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("recommend")){
                    bs.setRecommend(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("prices")){
                    bs.setPrices(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        close();
        return books;
    }

    public List<ResultItem> lookresultTable(Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "result"+title;
        List<ResultItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            ResultItem bs = new ResultItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ranking")){
                    String fg = Bytes.toString(CellUtil.cloneValue(cell));
                    bs.setRanking(fg.substring(0,fg.length()-2));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                    bs.setTitle(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        close();
        return books;
    }

    public List<ResultItem> resultTable()throws IOException{
        String tablename = "result1";
        List<ResultItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            ResultItem bs = new ResultItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("ranking")){
                    String fg = Bytes.toString(CellUtil.cloneValue(cell));
                    bs.setRanking(fg.substring(0,fg.length()-2));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                    bs.setTitle(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        close();
        return books;
    }











































































    public List<PulishItem> scanPublishTable()throws IOException{
        String tablename = "publish1";
        List<PulishItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            PulishItem bs = new PulishItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("number")){
                    bs.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<PulishItem> lookscanPublishTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "publish"+title;
        List<PulishItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            PulishItem bs = new PulishItem();
            //5.解析result
            bs.setRowKey(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("number")){
                    bs.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<FlowBeans> scanTuijianTable()throws IOException{
        String tablename = "tuijian1";
        List<FlowBeans> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            FlowBeans bs = new FlowBeans();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                        bs.setName(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("price")){
                    bs.setPrice(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<FlowBeans> lookscanTuijianTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "tuijian"+title;
        List<FlowBeans> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            FlowBeans bs = new FlowBeans();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")){
                    bs.setName(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("price")){
                    bs.setPrice(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<FlowBeans> scanCommentTable()throws IOException{
        String tablename = "comment1";
        List<FlowBeans> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            FlowBeans bs = new FlowBeans();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("name")){
                    bs.setName(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("price")){
                    bs.setPrice(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<FlowBeans> lookscanCommentTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "comment"+title;
        List<FlowBeans> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            FlowBeans bs = new FlowBeans();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("name")){
                    bs.setName(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("price")){
                    bs.setPrice(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }
    public List<PulishItem> scanbaituijianTable()throws IOException{
        String tablename = "baituijian1";
        List<PulishItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            PulishItem bs = new PulishItem();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("number")){
                    bs.setNumber((Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<PulishItem> lookscanbaituijianTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "baituijian"+title;
        List<PulishItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            PulishItem bs = new PulishItem();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("number")){
                    bs.setNumber((Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<BookItems2> scanundertwojobTable()throws IOException{
        String tablename = "undertwojob1";
        List<BookItems2> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            BookItems2 bs = new BookItems2();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setCommenttimes(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("tuijian")){
                    bs.setPrice(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<BookItems2> lookundertwojobTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "undertwojob"+title;
        List<BookItems2> books= new ArrayList<>();
//1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            BookItems2 bs = new BookItems2();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setCommenttimes(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("tuijian")){
                    bs.setPrice(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }










    public List<CommentTuijian> scancommentdatazhishujobTable()throws IOException{
        String tablename = "commentdatazhishujob1";
        List<CommentTuijian> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            CommentTuijian bs = new CommentTuijian();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("tuijian")){
                    bs.setTuijian(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<CommentTuijian> lookcommentdatazhishujobTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "commentdatazhishujob"+title;
        List<CommentTuijian> books= new ArrayList<>();
//1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            CommentTuijian bs = new CommentTuijian();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("comment")){
                    bs.setComment(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("tuijian")){
                    bs.setTuijian(Double.parseDouble(Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }











    public List<PulishItem> scanComTable()throws IOException{
        String tablename = "commentsum1";
        List<PulishItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            PulishItem bs = new PulishItem();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("number")){
                    bs.setNumber((Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }



    public List<PulishItem> lookComTable(@RequestBody Map<String,Object> map)throws IOException{
        String title = ((String)map.get("searchText"));
        String tablename = "commentsum"+title;
        List<PulishItem> books= new ArrayList<>();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            PulishItem bs = new PulishItem();
            //5.解析result
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("publisher")){
                    bs.setPublisher(Bytes.toString(CellUtil.cloneValue(cell)));
                }
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("number")){
                    bs.setNumber((Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
            books.add(bs);
        }
        table.close();
        admin.close();
        return books;
    }
}
