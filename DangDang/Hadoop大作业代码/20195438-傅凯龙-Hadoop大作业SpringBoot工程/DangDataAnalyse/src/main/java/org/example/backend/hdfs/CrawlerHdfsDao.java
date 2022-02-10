package org.example.backend.hdfs;
import org.example.backend.utils.SizeCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.stereotype.Component;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


@Component
public class CrawlerHdfsDao {
    private static SizeCount fileSize = new SizeCount();

    private FileSystem fs;
    //开启连接
    public void start() throws URISyntaxException, InterruptedException, URISyntaxException, IOException {
        URI uri  = new URI("hdfs://192.168.10.100:9000");
        Configuration configuration = new Configuration();
        String user = "hadoop";
        fs = FileSystem.get(uri,configuration,user);
    }

    //关闭连接
    public void close()throws IOException{
        fs.close();
    }
    public void deleteDictionary(String way)throws URISyntaxException, IOException,InterruptedException{

        /*
        copyToLocalFile(boolean delSrc, Path src, Path dst,
                        boolean useRawLocalFileSystem)
         */
        System.out.println("文件夹路径:");
        fs.delete(new Path(way),true);

    }
    public void exceltotxts() throws IOException, URISyntaxException, InterruptedException {
        this.start();
        for(int gh=1;gh<=9;gh++) {
            String url2 = System.getProperty("user.dir");
            String url3 = "\\book" + String.valueOf(gh) + ".xls";
            Workbook wb = null;
            Sheet sheet = null;
            Row row = null;
            List<Map<String, String>> list = null;
            String cellData = null;
            String excelPath = url2 + url3;
            String textPath = url2 + "\\book1totals" + String.valueOf(gh) + ".txt";
            File gko = new File(textPath);
            if(gko.exists()){
                gko.delete();}
            else{

            }
            String columns[] = {"ranking", "picture", "title", "comment", "recommend", "author", "publisher", "price"};
            wb = readExcel(excelPath);
            if (wb != null) {
                // 用来存放表中数据
                list = new ArrayList<Map<String, String>>();
                // 获取第一个sheet
                sheet = wb.getSheetAt(0);
                // 获取最大行数
                int rownum = sheet.getPhysicalNumberOfRows();
                // 获取第二行

                for (int i = 0; i < rownum; i++) {
                    Map<String, String> map = new LinkedHashMap<String, String>();
                    row = sheet.getRow(i);
                    if (row != null) {
                        for (int j = 0; j <= 7; j++) {
                            cellData = (String) getCellFormatValue(row.getCell(j));

                            String regEx="[\n`~!@#$%@#￥%……&*+|{}【】]";
                            String aa = "";
                            String newString = cellData.replaceAll(regEx,aa);

                            map.put(columns[j], newString);
                        }
                    } else {
                        break;
                    }
                    list.add(map);
                }
            }
            // 遍历解析出来的list
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < list.size(); i++) {
                for (Entry<String, String> entry : list.get(i).entrySet()) {
                    String value = entry.getValue();
                    String set = entry.getKey();
                    if (set.equals("ranking")) {
                        sb.append(value + "   ");
                    }
                    if (set.equals("picture")) {
                        sb.append(value + "   ");
                    }
                    if (set.equals("title")) {
                        sb.append(value + "   ");
                    }
                    if (set.equals("comment")) {
                        sb.append(value + "   ");
                    }
                    if (set.equals("recommend")) {
                        sb.append(value + "   ");
                    }
                    if (set.equals("author")) {
                        sb.append(value + "   ");
                    }
                    if (set.equals("publisher")) {
                        sb.append(value + "   ");
                    }
                    if (set.equals("price")) {
                        sb.append(value + "   ");
                    }
                }
                sb.append("\r\n");
            }
            WriteToFile(sb.toString(), textPath);
            String wsyss = "\\dataanalyse\\crawler\\MergePublish\\book\\" + String.valueOf(gh) + "total.txt";
            Path delPath = new Path(wsyss);
            deleteDictionary(wsyss);
            System.out.println(wsyss);
            fs.copyFromLocalFile(false, true, new Path(textPath),
                    new Path(wsyss));

        }
        this.close();
    }
    public void exceltotxt() throws IOException, URISyntaxException, InterruptedException {
        this.start();
        for(int gh=1;gh<=9;gh++) {

            String url2 = System.getProperty("user.dir");
            String url3 = "\\book" + String.valueOf(gh) + ".xls";
            Workbook wb = null;
            Sheet sheet = null;
            Row row = null;
            List<Map<String, String>> list = null;
            String cellData = null;
            String excelPath = url2 + url3;
            String textPath = url2 + "\\book" + String.valueOf(gh) + ".txt";
            String columns[] = {"ranking", "picture", "title", "comment", "recommend", "author", "publisher", "price"};
            wb = readExcel(excelPath);
            if (wb != null) {
                // 用来存放表中数据
                list = new ArrayList<Map<String, String>>();
                // 获取第一个sheet
                sheet = wb.getSheetAt(0);
                // 获取最大行数
                int rownum = sheet.getPhysicalNumberOfRows();
                // 获取第二行

                for (int i = 0; i < rownum; i++) {
                    Map<String, String> map = new LinkedHashMap<String, String>();
                    row = sheet.getRow(i);
                    if (row != null) {
                        for (int j = 6; j < 7; j++) {
                            cellData = (String) getCellFormatValue(row.getCell(j));
                            map.put(columns[j], cellData);
                        }
                    } else {
                        break;
                    }
                    list.add(map);
                }
            }
            // 遍历解析出来的list
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < list.size(); i++) {
                for (Entry<String, String> entry : list.get(i).entrySet()) {
                    String value = entry.getValue();
                    String set = entry.getKey();
                    if (set.equals("publisher")) {
                        sb.append(value + " ");
                    }
                }
                sb.append("\r\n");
            }
            WriteToFile(sb.toString(), textPath);
            String wsyss = "\\dataanalyse\\crawler\\MergePublish\\book\\" + String.valueOf(gh) + ".txt";
            Path delPath = new Path(wsyss);
            deleteDictionary(wsyss);
            System.out.println(wsyss);
            fs.copyFromLocalFile(false, true, new Path(textPath),
                    new Path(wsyss));

        }
        this.close();
    }
    public Workbook readExcel(String filePath) {
        Workbook wb = null;
        if (filePath == null) {
            return null;
        }
        String extString = filePath.substring(filePath.lastIndexOf("."));
        InputStream is = null;
        try {
            is = new FileInputStream(filePath);
            if (".xls".equals(extString)) {
                return wb = new HSSFWorkbook(is);
            } else if (".xlsx".equals(extString)) {
                return wb = new XSSFWorkbook(is);
            } else {
                return wb = null;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return wb;
    }
    public  Object getCellFormatValue(Cell cell) {
        Object cellValue = null;
        if (cell != null) {
            // 判断cell类型
            switch (cell.getCellType()) {
                case Cell.CELL_TYPE_NUMERIC: {
                    cellValue = String.valueOf(cell.getNumericCellValue());
                    break;
                }
                case Cell.CELL_TYPE_FORMULA: {
                    // 判断cell是否为日期格式
                    if (DateUtil.isCellDateFormatted(cell)) {
                        // 转换为日期格式YYYY-mm-dd
                        cellValue = cell.getDateCellValue();
                    } else {
                        // 数字
                        cellValue = String.valueOf(cell.getNumericCellValue());
                    }
                    break;
                }
                case Cell.CELL_TYPE_STRING: {
                    cellValue = cell.getRichStringCellValue().getString();
                    break;
                }
                default:
                    cellValue = "";
            }
        } else {
            cellValue = "";
        }
        return cellValue;
    }
    public void WriteToFile(String str, String filePath) throws IOException {
        BufferedWriter bw = null;
        try {
            FileOutputStream out = new FileOutputStream(filePath, true);// true,表示:文件追加内容，不重新生成,默认为false
            bw = new BufferedWriter(new OutputStreamWriter(out, "GBK"));
            bw.write(str += "\r\n");// 换行
            bw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bw.close();
        }
    }

    //文件上传
    public void filePut()throws URISyntaxException, IOException,InterruptedException{
        for(int g=1;g<=12;g++) {
            String url2 = System.getProperty("user.dir");
            String url3 = "\\book"+String.valueOf(g)+".xls";
            this.start();
            File kfs = new File(url2+url3);
            Path path = new Path("\\dataanalyse\\crawler\\totalbook\\book");
            FileStatus[] list = fs.listStatus(path);
            Double mmm=(Double)0.0;
            String mgm="";
            List<String> mk = new ArrayList<>();
            for (FileStatus fileStatus : list) {
                String namehome = fileStatus.getPath().getName();
                Double resultks = Double.valueOf(namehome.substring(namehome.length()-18,namehome.length()-4));
               if(resultks>=mmm)
               {
                   mmm=resultks;
                   mgm=namehome;
               }
            }
            if(!kfs.exists())
            {
                Path iop = new Path("\\dataanalyse\\crawler\\totalbook\\book\\"+mgm);
                fs.copyToLocalFile(false,iop,
                        new Path(url2+url3),true);
            }
            else {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                String datatime = sdf.format(timestamp);
                String way = url2 + url3;
                System.out.println(way);
                String wayja = "\\dataanalyse\\crawler\\totalbook\\book\\" + String.valueOf(g) + "_" + datatime + ".xls";
                System.out.println(wayja);
                fs.copyFromLocalFile(false, true, new Path(way),
                        new Path(wayja));
            }
        }
        this.close();
    }
    public void resultPut()throws URISyntaxException, IOException,InterruptedException{
        for(int g=1;g<=3;g++) {
            this.start();
            String url2 = System.getProperty("user.dir");
            String url3 = "\\result"+String.valueOf(g)+".xls";
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String datatime = sdf.format(timestamp);
            String way = url2 + url3;
            System.out.println(way);
            String wayja = "\\dataanalyse\\machinelearn\\result" +String.valueOf(g)+"_"+ datatime + ".xls";
            System.out.println(wayja);
            fs.copyFromLocalFile(false, true, new Path(way),
                    new Path(wayja));
        }
        this.close();
    }
    //新建文件夹
//    public void newDictionary()throws URISyntaxException, IOException,InterruptedException{
//        this.start();
//        /*
//        copyToLocalFile(boolean delSrc, Path src, Path dst,
//                        boolean useRawLocalFileSystem)
//         */
//        System.out.println("文件夹路径:");
//        String way = Main.sc.nextLine();
//        fs.mkdirs(new Path(way));
//        System.out.println(fs.getUri());
//        this.close();
//    }
    //删除文件夹
//    public void deleteDictionary()throws URISyntaxException, IOException,InterruptedException{
//        this.start();
//        /*
//        copyToLocalFile(boolean delSrc, Path src, Path dst,
//                        boolean useRawLocalFileSystem)
//         */
//        System.out.println("文件夹路径:");
//        String way = Main.sc.nextLine();
//        fs.delete(new Path(way),true);
//        this.close();
//    }
    //查看文件夹
//    public void lookDictionary()throws URISyntaxException, IOException,InterruptedException{
//        this.start();
//        System.out.println("文件夹路径:");
//        String folder = Main.sc.nextLine();
//        Path path = new Path(folder);
//        FileStatus[] list = fs.listStatus(path);
//        System.out.println("ls: " + folder);
//        System.out.println("==========================================================");
//        for (FileStatus fileStatus : list) {
//            System.out.println("------------------------------------------------------------");
//            System.out.println("文件名称:"+fileStatus.getPath().getName());
//            System.out.println("文件地址："+fileStatus.getPath());
//            System.out.println("操作权限:"+fileStatus.getPermission());
//            System.out.println("文件所有者:"+fileStatus.getOwner());
//            System.out.println("文件大小:"+fileSize.filesizecount(fileStatus.getLen()));
//            System.out.println("文件最大占用空间:"+fileSize.filesizecount(fileStatus.getBlockSize()));
//            System.out.println("------------------------------------------------------------");
//        }
//        System.out.println("==========================================================");
//        this.close();
//    }
    //文件下载
    public void fileGet()throws URISyntaxException, IOException,InterruptedException{
        this.start();
        String folder = "\\dataanalyse\\crawler\\totalbook\\bookone";
        String maxls = "";
        Double max = (double)0;
        Path path = new Path(folder);
        FileStatus[] list = fs.listStatus(path);
        for (FileStatus fileStatus : list) {
            Double doubles = new Double(fileStatus.getPath().getName());
            if (doubles > max){
                max = doubles;
                maxls = fileStatus.getPath().getName();
            }
        }
        String wayone = folder+"\\"+maxls+".xls";
        String url2 = System.getProperty("user.dir");
        String url3 = "\\book.xls";
        String waytwo = url2+url3;
        /*
        copyToLocalFile(boolean delSrc, Path src, Path dst,
                        boolean useRawLocalFileSystem)
         */
        fs.copyToLocalFile(false,new Path(wayone),
                new Path(waytwo),true);
        this.close();
    }

    //读取文件
//    public List<String> getFiles(String dir) throws IOException, URISyntaxException, InterruptedException {
//        Path path = new Path(dir);
//        List<String> filelist = new ArrayList<>();
//        try {
//            //对单个文件或目录下所有文件和目录
//            FileStatus[] fileStatuses = fs.listStatus(path);
//
//            for (FileStatus fileStatus : fileStatuses) {
//                //递归查找子目录
//                if (fileStatus.isDirectory()) {
//                    filelist.addAll(getFiles(fileStatus.getPath().toString()));
//                } else {
//                    filelist.add(fileStatus.getPath().toString());
//                }
//            }
//            return filelist;
//        } finally {
//        }
//    }
    //显示文件内容
//    public String cat() throws IOException, URISyntaxException, InterruptedException {
//        this.start();
//        System.out.println("请输入hdfs文件路径:");
//        String remoteFile = Main.sc.nextLine();
//        Path path = new Path(remoteFile);
//        FSDataInputStream fsdis = null;
//        System.out.println("cat: " + remoteFile);
//
//        OutputStream baos = new ByteArrayOutputStream();
//        String str = null;
//
//        try {
//            fsdis = fs.open(path);
//            IOUtils.copyBytes(fsdis, baos, 4096, false);
//            str = baos.toString();
//        } finally {
//            IOUtils.closeStream(fsdis);
//            fs.close();
//        }
//        System.out.println(str);
//        this.close();
//        return str;
//    }


    //小文件合并还原
//    public void extractCombineSequenceFile() throws IOException, URISyntaxException, InterruptedException {
//        this.start();
//        System.out.println("原文件目录:");
//        String sourceFile = Main.sc.nextLine();
//
//        Configuration confy = new Configuration();
//        Path sourcePath = new Path(sourceFile);
//
//        SequenceFile.Reader reader = null;
//        SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(sourcePath);
//
//        Writable key = null;
//        Writable value = null;
////        Text key = null;
////        BytesWritable value = null;
//
//        FileSystem fs = FileSystem.get(confy);
//        try {
//            reader = new SequenceFile.Reader(confy, option1);
//            key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), confy);
//            value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), confy);
//
//            //在知道key和value的明确类型的情况下，可以直接用其类型
////            key = ReflectionUtils.newInstance(Text.class, conf);
////            value =  ReflectionUtils.newInstance(BytesWritable.class, conf);
//            long position = reader.getPosition();
//            while (reader.next(key, value)) {
//                FSDataOutputStream out = fs.create(new Path(key.toString()), true);
//                //文件头会多出4个字节，用来标识长度，而本例中原文件头是没有长度的，所以不能用这个方式写入流
////                value.write(out);
//                out.write(((BytesWritable)value).getBytes(),0,((BytesWritable)value).getLength());
//
//                //                out.write(value.getBytes(),0,value.getLength());
//                System.out.printf("[%s]\t%s\t%s\n", position, key, out.getPos());
//                out.close();
//                position = reader.getPosition();
//            }
//        } finally {
//            IOUtils.closeStream(reader);
//            IOUtils.closeStream(fs);
//            this.close();
//        }
//    }
   //小文件合并
//    public void combineToSequenceFile() throws IOException, URISyntaxException, InterruptedException {
//        this.start();
//        System.out.println("原文件目录:");
//        String sourceDir = Main.sc.nextLine();
//        System.out.println("目的目录和文件名:");
//        String destFile = Main.sc.nextLine();
//        Configuration confs = new Configuration();
//        List<String> files = getFiles(sourceDir);
//
//        Path destPath = new Path(destFile);
//        if (fs.exists(destPath)) {
//            fs.delete(destPath, true);
//        }
//
//        FSDataInputStream in = null;
//
//        Text key = new Text();
//        BytesWritable value = new BytesWritable();
//
//        byte[] buff = new byte[4096];
//        SequenceFile.Writer writer = null;
//
//        SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(new Path(destFile));
//        SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(key.getClass());
//        SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(value.getClass());
//        SequenceFile.Writer.Option option4 = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
//        try {
//            writer = SequenceFile.createWriter(confs, option1, option2, option3, option4);
//            for (int i = 0; i < files.size(); i++) {
//                Path path = new Path(files.get(i).toString());
//                System.out.println("读取文件：" + path.toString());
//                key = new Text(files.get(i).toString());
//                in = fs.open(path);
////                只能处理小文件，int最大只能表示到1个G的大小，实际上大文件放入SequenceFile也没有意义
//                int length = (int) fs.getFileStatus(path).getLen();
//                byte[] bytes = new byte[length];
////                read最多只能读取65536的大小
//                int readLength = in.read(buff);
//                int offset = 0;
//                while (readLength > 0) {
//                    System.arraycopy(buff, 0, bytes, offset, readLength);
//                    offset += readLength;
//                    readLength = in.read(buff);
//                }
//                System.out.println("file length:" + length + ",read length:" + offset);
//                value = new BytesWritable(bytes);
//                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value.getLength());
//                writer.append(key, value);
//            }
//        } finally {
//            IOUtils.closeStream(in);
//            IOUtils.closeStream(writer);
//            IOUtils.closeStream(fs);
//
//            this.close();
//        }
//
//    }
//    public void raname()  throws  Exception {
//        this.start();
//        System.out.println("原文件路径:");
//        String frpahts = Main.sc.nextLine();
//        System.out.println("新文件路径:");
//        String topaths = Main.sc.nextLine();
//        Path frpaht= new  Path( frpahts );     //旧的文件名
//        Path topath= new  Path( topaths );     //新的文件名
//        boolean  isRename=fs.rename(frpaht, topath);
//        String result=isRename? "成功" : "失败" ;
//        System.out.println( "文件重命名结果为：" +result);
//        this.close();
//    }
//    public void filelocation() throws IOException, URISyntaxException, InterruptedException {
//        this.start();
//        System.out.println("文件路径");
//        String way = Main.sc.nextLine();
//        if(fs.exists(new Path(way))) {
//            FileStatus f = fs.getFileStatus(new Path(way));
//            BlockLocation[] list = fs.getFileBlockLocations(f, 0, f.getLen());
//
//            System.out.println("File Location: " + way);
//            for (BlockLocation bl : list) {
//                String[] hosts = bl.getHosts();
//                for (String host : hosts) {
//                    System.out.println("host:" + host);
//                }
//            }
//        }
//        else{
//            System.out.println("路径不存在");
//        }
//        this.close();
//    }
//    public void getTotalCapacity() throws IOException, URISyntaxException, InterruptedException {
//        this.start();
//        try {
//            FsStatus fsStatus = fs.getStatus();
//            System.out.println("总容量:" + fsStatus.getCapacity());
//            System.out.println("使用容量:" + fsStatus.getUsed());
//            System.out.println("剩余容量:" + fsStatus.getRemaining());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        this.close();
//    }
//    public void recursiveHdfsPath() throws URISyntaxException, IOException, InterruptedException {
//        this.start();
//        Set<String> set = new TreeSet<>();
//                FileStatus[] files = null;
//                try {
//                    files = hdfs.listStatus(listPath);
//                    Path[] paths = FileUtil.stat2Paths(files);
//                    for(int i=0;i<files.length;i++){
//                        if(files[i].isFile()){
//                            // set.add(paths[i].toString());
//                            set.add(paths[i].getName());
//                        }else {
//                            recursiveHdfsPath(hdfs,paths[i]);
//                        }
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    logger.error(e);
//                }
//
//        FileStatus[] files = null;
//        System.out.println("请输入文件夹路径");
//        String ways = Main.sc.nextLine();
//        Path listPath = new Path(ways);
//        try {
//            files = fs.listStatus(listPath);
//            // 实际上并不是每个文件夹都会有文件的。
//            if(files.length == 0){
//                // 如果不使用toUri()，获取的路径带URL。
//                set.add(listPath.toUri().getPath());
//            }else {
//                // 判断是否为文件
//                for (FileStatus f : files) {
//                    if (files.length == 0 || f.isFile()) {
//                        set.add(f.getPath().toUri().getPath());
//                    } else {
//                        // 是文件夹，且非空，就继续遍历
//                        recursiveHdfsPath();
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        for(String way:set)
//        {
//           System.out.println("文件路径:"+way);
//        }
//        this.close();
//    }
//    public void copyFileBetweenHDFS() throws URISyntaxException, IOException, InterruptedException {
//        this.start();
//        System.out.println("原文件路径:");
//        String frpahts = Main.sc.nextLine();
//        System.out.println("新文件路径:");
//        String topaths = Main.sc.nextLine();
//        Path inPath = new Path(frpahts);
//        Path outPath = new Path(topaths);
//        // byte[] ioBuffer = new byte[1024*1024*64];
//        // int len = 0;
//        FSDataInputStream hdfsIn = null;
//        FSDataOutputStream hdfsOut = null;
//        try {
//            hdfsIn = fs.open(inPath);
//            hdfsOut = fs.create(outPath);
//
//            IOUtils.copyBytes(hdfsIn,hdfsOut,1024*1024*64,false);
//
//                    /*while((len=hdfsIn.read(ioBuffer))!= -1){
//                        IOUtils.copyBytes(hdfsIn,hdfsOut,len,true);
//                    }*/
//        } catch (IOException e) {
//            e.printStackTrace();
//        }finally {
//            try {
//                hdfsOut.close();
//                hdfsIn.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            this.close();
//        }
//
//    }


}
