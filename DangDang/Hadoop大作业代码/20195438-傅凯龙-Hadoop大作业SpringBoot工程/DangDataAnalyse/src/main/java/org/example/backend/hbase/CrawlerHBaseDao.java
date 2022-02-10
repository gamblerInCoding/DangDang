package org.example.backend.hbase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
@Component
public class CrawlerHBaseDao {
    public static final Scanner sc = new Scanner(System.in);
    private static HBaseAdmin admin = null;
    // 定义配置对象HBaseConfiguration
    private static Configuration configuration;
    private static Connection connection;
    public static CrawlerHBaseDao crawlerHBaseDao;



    public void start()throws IOException{
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        connection = ConnectionFactory.createConnection(configuration);
        admin = (HBaseAdmin) connection.getAdmin();
    }








    public List getAllTables()throws IOException{
        this.start();
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        admin.close();
        return tables;
    }


    public void createTableTest(int i) throws IOException {
        // 表名
        this.start();
        System.out.println("表名:");
        String TABLE_NAME = "publish"+String.valueOf(i);
        System.out.println("列族名:");
        String COLUMN_FAMILY = "1";

        // 2. 构建表描述构建器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));

        // 3. 构建列蔟描述构建器
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
        columnFamilyDescriptorBuilder.setInMemory(true);
        columnFamilyDescriptorBuilder.setMaxVersions(3);
        columnFamilyDescriptorBuilder.setTimeToLive(2 * 24 * 60 * 60);
        // 4. 构建列蔟描述
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        // 5. 构建表描述
        // 添加列蔟
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        // 6. 创建表
        admin.createTable(tableDescriptor);
        admin.close();
    }

    public void createTable() throws IOException {
        //设置表名
        this.start();
        TableName tableName = TableName.valueOf("xuehu01");

//将表名传递给HTableDescriptor

        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

//创建列族

        HColumnDescriptor one = new HColumnDescriptor("range");
        HColumnDescriptor two = new HColumnDescriptor("imag");
        HColumnDescriptor three = new HColumnDescriptor("title");
        HColumnDescriptor four = new HColumnDescriptor("recommand");
        HColumnDescriptor five = new HColumnDescriptor("author");
        HColumnDescriptor six = new HColumnDescriptor("times");
        HColumnDescriptor seven = new HColumnDescriptor("prices");
//将列族添加进表中

        hTableDescriptor.addFamily(one);
        hTableDescriptor.addFamily(two);
        hTableDescriptor.addFamily(three);
        hTableDescriptor.addFamily(four);
        hTableDescriptor.addFamily(five);
        hTableDescriptor.addFamily(six);
        hTableDescriptor.addFamily(seven);
        admin.createTable(hTableDescriptor);
        admin.close();
    }
    public void scanTable()throws IOException{
        this.start();
        System.out.println("表名:");
        String tablename = sc.nextLine();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            //5.解析result
            System.out.println("rowkey => " + Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()){
                //6.打印数据
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title")) {
                    System.out.println("CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

        }
        //7.关闭
        table.close();
        admin.close();
    }
    public void scanManyTable()throws IOException{
        this.start();
        System.out.println("表名:");
        String tablename = sc.nextLine();
        System.out.println("书名:");
        String books = sc.nextLine();
        System.out.println("作者名:");
        String authors = sc.nextLine();
        try {
            //1.获取表对象
            Table table = connection.getTable(TableName.valueOf(tablename));
            List filters = new ArrayList();
            Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("title"), null, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(books));
            filters.add(filter1);
            Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("author"), null, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(authors));
            filters.add(filter2);
            FilterList filterList1 = new FilterList(filters);
            Scan scan = new Scan();
            scan.setFilter(filterList1);
            ResultScanner rs = table.getScanner(scan);
            for(Result result:rs){
                //5.解析result
                for (Cell cell : result.rawCells()){
                    //6.打印数据
                    System.out.println("CN:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            System.out.println(scan.getFilter());
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        admin.close();
    }

    public void scanOneTable()throws IOException{
        this.start();
        System.out.println("表名:");
        String tablename = sc.nextLine();
        System.out.println("书名:");
        String title = sc.nextLine();
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tablename));

        //2.构建Scan对象
        Scan scan = new Scan();


        //3.扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4.解析result
        for(Result result:resultScanner){
            //5.解析result
            for (Cell cell : result.rawCells()){
                if((Bytes.toString(CellUtil.cloneQualifier(cell)).equals("title"))&&((Bytes.toString(CellUtil.cloneValue(cell))).equals(title))) {
                    //6.打印数据
                    for (Cell cells : result.rawCells()){

                        System.out.println("CN:" + Bytes.toString(CellUtil.cloneQualifier(cells)));
                        System.out.println("Value:" + Bytes.toString(CellUtil.cloneValue(cells)));

                    }
                    break;
                }
            }

        }
        //7.关闭
        table.close();
        admin.close();

    }

    public void putData()throws IOException{

        this.start();

       List<String>name =  this.getAllTables();
        System.out.println("表名:");
        String tablenames = sc.nextLine();
        System.out.println("列族名:");
        String dur = sc.nextLine();
        File file = new File("src/main/resources/书籍表.xls");
        HSSFWorkbook workbook = new HSSFWorkbook(FileUtils.openInputStream(file));
        HSSFSheet sheet = workbook.getSheetAt(0);
        System.out.println(sheet.getLastRowNum());
        TableName tablename = TableName.valueOf(tablenames);
        Table table = connection.getTable(tablename);
        for(int i=0;i<=sheet.getLastRowNum()/10;i++)
        {
            System.out.println(sheet.getRow(i).getCell(2).toString());
            Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
            put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("range"),Bytes.toBytes(sheet.getRow(i).getCell(0).toString()));
            put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("image"),Bytes.toBytes(sheet.getRow(i).getCell(1).toString()));
            put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("title"),Bytes.toBytes(sheet.getRow(i).getCell(2).toString()));
            put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("recommand"),Bytes.toBytes(sheet.getRow(i).getCell(3).toString()));
            put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("author"),Bytes.toBytes(sheet.getRow(i).getCell(4).toString()));
            put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("times"),Bytes.toBytes(sheet.getRow(i).getCell(5).toString()));
            put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("prices"),Bytes.toBytes(sheet.getRow(i).getCell(6).toString()));
            table.put(put);
        }
        //5.关闭连接
        table.close();
        admin.close();
    }






    public void updateData()throws IOException{
        this.start();
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.10.100");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        connection = ConnectionFactory.createConnection(configuration);
        admin = (HBaseAdmin) connection.getAdmin();
        int  mt=0;
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        for(int p=1;p<=9;p++)
        {
            mt=0;
            String url1 = System.getProperty("user.dir");
            String url2 = "\\book"+String.valueOf(p)+".xls";
            String tablenames = "book"+String.valueOf(p)+"month";
            for(String name:tables){
                if(name.equals(tablenames)){
                    mt=1;
                    break;
                }
            }
            System.out.println("mt"+mt);
            if(mt==1)
            {
                admin.disableTable(TableName.valueOf(tablenames));
                admin.deleteTable(TableName.valueOf(tablenames));
                String TABLE_NAME = tablenames;
                String COLUMN_FAMILY = "1";
                // 2. 构建表描述构建器
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
                // 3. 构建列蔟描述构建器
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                columnFamilyDescriptorBuilder.setInMemory(true);
                columnFamilyDescriptorBuilder.setMaxVersions(3);
                columnFamilyDescriptorBuilder.setTimeToLive(2 * 24 * 60 * 60);
                // 4. 构建列蔟描述
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                // 5. 构建表描述
                // 添加列蔟
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
                TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
                // 6. 创建表
                admin.createTable(tableDescriptor);
            }
            else{
                String TABLE_NAME = tablenames;
                String COLUMN_FAMILY = "1";
                // 2. 构建表描述构建器
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
                // 3. 构建列蔟描述构建器
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                columnFamilyDescriptorBuilder.setInMemory(true);
                columnFamilyDescriptorBuilder.setMaxVersions(3);
                columnFamilyDescriptorBuilder.setTimeToLive(2 * 24 * 60 * 60);
                // 4. 构建列蔟描述
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                // 5. 构建表描述
                // 添加列蔟
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
                TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
                // 6. 创建表
                admin.createTable(tableDescriptor);
            }
            String dur = "1";
            File file = new File(url1+url2);
            HSSFWorkbook workbook = new HSSFWorkbook(FileUtils.openInputStream(file));
            HSSFSheet sheet = workbook.getSheetAt(0);
            TableName tablename = TableName.valueOf(tablenames);
            Table table = connection.getTable(tablename);
            for(int i=0;i<=sheet.getLastRowNum();i++)
            {
                Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("ranking"),Bytes.toBytes(sheet.getRow(i).getCell(0).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("title"),Bytes.toBytes(sheet.getRow(i).getCell(2).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("author"),Bytes.toBytes(sheet.getRow(i).getCell(5).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("publisher"),Bytes.toBytes(sheet.getRow(i).getCell(6).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("prices"),Bytes.toBytes(sheet.getRow(i).getCell(7).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("comment"),Bytes.toBytes(sheet.getRow(i).getCell(3).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("recommend"),Bytes.toBytes(sheet.getRow(i).getCell(4).toString()));
                table.put(put);
            }
            //5.关闭连接
            table.close();
        }
        for(int p=10;p<=12;p++)
        {
            mt=0;
            String url1 = System.getProperty("user.dir");
            String url2 = "\\book"+String.valueOf(p)+".xls";
            String tablenames = "book"+String.valueOf(p)+"month";
            for(String name:tables){
                if(name.equals(tablenames)){
                    mt=1;
                    break;
                }
            }
            if(mt==1)
            {
                admin.disableTable(TableName.valueOf(tablenames));
                admin.deleteTable(TableName.valueOf(tablenames));
                String TABLE_NAME = tablenames;
                String COLUMN_FAMILY = "1";
                // 2. 构建表描述构建器
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
                // 3. 构建列蔟描述构建器
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                columnFamilyDescriptorBuilder.setInMemory(true);
                columnFamilyDescriptorBuilder.setMaxVersions(3);
                columnFamilyDescriptorBuilder.setTimeToLive(2 * 24 * 60 * 60);
                // 4. 构建列蔟描述
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                // 5. 构建表描述
                // 添加列蔟
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
                TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
                // 6. 创建表
                admin.createTable(tableDescriptor);
            }
            else{
                String TABLE_NAME = tablenames;
                String COLUMN_FAMILY = "1";
                // 2. 构建表描述构建器
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
                // 3. 构建列蔟描述构建器
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                columnFamilyDescriptorBuilder.setInMemory(true);
                columnFamilyDescriptorBuilder.setMaxVersions(3);
                columnFamilyDescriptorBuilder.setTimeToLive(2 * 24 * 60 * 60);
                // 4. 构建列蔟描述
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                // 5. 构建表描述
                // 添加列蔟
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
                TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
                // 6. 创建表
                admin.createTable(tableDescriptor);
            }
            String dur = "1";
            File file = new File(url1+url2);
            HSSFWorkbook workbook = new HSSFWorkbook(FileUtils.openInputStream(file));
            HSSFSheet sheet = workbook.getSheetAt(0);
            System.out.println(sheet.getLastRowNum());
            TableName tablename = TableName.valueOf(tablenames);
            Table table = connection.getTable(tablename);
            for(int i=0;i<=sheet.getLastRowNum();i++)
            {
                Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("ranking"),Bytes.toBytes(sheet.getRow(i).getCell(0).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("title"),Bytes.toBytes(sheet.getRow(i).getCell(2).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("author"),Bytes.toBytes(sheet.getRow(i).getCell(5).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("publisher"),Bytes.toBytes(sheet.getRow(i).getCell(6).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("prices"),Bytes.toBytes(sheet.getRow(i).getCell(7).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("comment"),Bytes.toBytes(sheet.getRow(i).getCell(3).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("recommend"),Bytes.toBytes(sheet.getRow(i).getCell(4).toString()));
                table.put(put);
            }
            //5.关闭连接
            table.close();

        }

        admin.close();
    }










    public void updateResult()throws IOException{
        this.start();
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        for(int p=1;p<=3;p++)
        {
            int mt=0;
            String url1 = System.getProperty("user.dir");
            String url2 = "\\result"+String.valueOf(p)+".xls";
            String tablenames = "result"+String.valueOf(p);
            for(String name:tables){
                if(name.equals(tablenames)){
                    mt = 1;
                    break;
                }
            }
            if(mt==1)
            {
                admin.disableTable(TableName.valueOf(tablenames));
                admin.deleteTable(TableName.valueOf(tablenames));
                String TABLE_NAME = tablenames;
                String COLUMN_FAMILY = "1";

                // 2. 构建表描述构建器
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));

                // 3. 构建列蔟描述构建器
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                columnFamilyDescriptorBuilder.setInMemory(true);
                columnFamilyDescriptorBuilder.setMaxVersions(3);
                columnFamilyDescriptorBuilder.setTimeToLive(2 * 24 * 60 * 60);
                // 4. 构建列蔟描述
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                // 5. 构建表描述
                // 添加列蔟
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
                TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
                // 6. 创建表
                admin.createTable(tableDescriptor);
            }
            else{
                String TABLE_NAME = tablenames;
                String COLUMN_FAMILY = "1";

                // 2. 构建表描述构建器
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));

                // 3.
                // 构建列蔟描述构建器
                ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
                columnFamilyDescriptorBuilder.setInMemory(true);
                columnFamilyDescriptorBuilder.setMaxVersions(3);
                columnFamilyDescriptorBuilder.setTimeToLive(2 * 24 * 60 * 60);
                // 4. 构建列蔟描述
                ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
                // 5. 构建表描述
                // 添加列蔟
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
                TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
                // 6. 创建表
                admin.createTable(tableDescriptor);
            }
            String dur = "1";
            File file = new File(url1+url2);
            HSSFWorkbook workbook = new HSSFWorkbook(FileUtils.openInputStream(file));
            HSSFSheet sheet = workbook.getSheetAt(0);
            System.out.println(sheet.getLastRowNum());
            TableName tablename = TableName.valueOf(tablenames);
            Table table = connection.getTable(tablename);
            for(int i=1;i<=sheet.getLastRowNum();i++)
            {
                Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("title"),Bytes.toBytes(sheet.getRow(i).getCell(0).toString()));
                put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("ranking"),Bytes.toBytes(sheet.getRow(i).getCell(1).toString()));

                table.put(put);
            }
            //5.关闭连接
            table.close();

        }



        admin.close();
    }



    public void putOneData()throws IOException{
        this.start();
        System.out.println("表名:");
        String tablenames = sc.nextLine();
        System.out.println("列族名:");
        String dur = sc.nextLine();
        System.out.println("排名:");
        String way = sc.nextLine();
        System.out.println("图片:");
        String mag = sc.nextLine();
        System.out.println("名字:");
        String names = sc.nextLine();
        System.out.println("推荐:");
        String recom = sc.nextLine();
        System.out.println("作者:");
        //5.关闭连接

        String auto = sc.nextLine();
        System.out.println("次数:");
        String tim = sc.nextLine();
        System.out.println("价格:");
        String pri = sc.nextLine();
        TableName tablename = TableName.valueOf(tablenames);
        Table table = connection.getTable(tablename);
        Put put = new Put(Bytes.toBytes(String.valueOf(Long.MAX_VALUE - System.currentTimeMillis())));
        put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("range"),Bytes.toBytes(way));
        put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("image"),Bytes.toBytes(mag));
        put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("title"),Bytes.toBytes(names));
        put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("recommand"),Bytes.toBytes(recom));
        put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("author"),Bytes.toBytes(auto));
        put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("times"),Bytes.toBytes(tim));
        put.addColumn(Bytes.toBytes(dur),Bytes.toBytes("prices"),Bytes.toBytes(pri));
        table.put(put);
        admin.close();
    }
    public void deleteTable(String tablenames) throws IOException {
        this.start();
        TableName tablename = TableName.valueOf(tablenames);
        admin.disableTable(tablename);
        admin.deleteTable(tablename);
        admin.close();
    }


    public void deleteOneTest() throws IOException {
        this.start();
        System.out.println("表名:");
        String tablename = sc.nextLine();
        System.out.println("rowKey:");
        String rowKey = sc.nextLine();
        // 1. 获取HTable对象
        Table waterBillTable = connection.getTable(TableName.valueOf(tablename));
        // 2. 根据rowkey构建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 3. 执行delete请求
        waterBillTable.delete(delete);
        // 4. 关闭表
        waterBillTable.close();
        admin.close();
    }
    public void deleteManyTest() throws IOException {
        this.start();
        String []rowKey = new String[9];
        System.out.println("表名:");
        String tablename = sc.nextLine();
        System.out.println("rowKey名:");
        String t = "";
        int timeww = 0;
        while(t.equals("over")) {
            rowKey[timeww] = sc.nextLine();
            timeww++;
        }
        // 1. 获取HTable对象
        Table waterBillTable = connection.getTable(TableName.valueOf(tablename));
        // 2. 根据rowkey构建delete对象
        for(String row:rowKey) {
            Delete delete = new Delete(Bytes.toBytes(row));
            waterBillTable.delete(delete);
        }
        // 4. 关闭表
        waterBillTable.close();
        admin.close();
    }
    public void getOneTest(String rowKey,String tablename) throws IOException {
        this.start();
        // 1. 获取HTable
        TableName waterBillTableName = TableName.valueOf(tablename);
        Table waterBilltable = connection.getTable(waterBillTableName);
        // 2. 使用rowkey构建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        // 3. 执行get请求
        Result result = waterBilltable.get(get);
        // 4. 获取所有单元格
        List<Cell> cellList = result.listCells();
        if(cellList!=null){
            // 打印rowkey
            System.out.println("rowkey => " + Bytes.toString(result.getRow()));
            // 5. 迭代单元格列表
            for (Cell cell : cellList) {
                //5.打印数据
                System.out.println("CN:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }else{
            System.out.println("未查找到该数据！");
        }
        // 6. 关闭表
        waterBilltable.close();
        admin.close();
    }
    public boolean isTableExist(String tableName) throws IOException{
        this.start();
        TableName table = TableName.valueOf(tableName);
        return admin.tableExists(table);

    }






}
