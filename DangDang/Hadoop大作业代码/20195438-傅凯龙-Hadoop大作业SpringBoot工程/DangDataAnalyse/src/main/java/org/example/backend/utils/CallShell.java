package org.example.backend.utils;
import org.example.backend.hdfs.CrawlerHdfsDao;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class CallShell {
    public static String call(String[] str) throws IOException {
        if (str == null || str.length == 0) {
            return null;
        }
        File file4 = new File(str[1]);
        String url1 = "python ";
        String url2 = System.getProperty("user.dir");
        String url3 = "\\src\\main\\java\\org\\example\\backend\\crawler\\pachong";
        String url = "";
        for(int i=1;i<=12;i++) {
            String url4 = String.valueOf(i) + ".py";
            url = url1 + url2 + url3 + url4;
            System.out.println(url);
            Process process = Runtime.getRuntime().exec(url);
            BufferedReader strCon = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            StringBuffer sb = new StringBuffer();
            while ((line = strCon.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
            strCon.close();
            process.destroy();
        }

            String urlx = "\\src\\main\\java\\org\\example\\backend\\machinelearn\\way1.py";
            Process processt = Runtime.getRuntime().exec(url1+url2+urlx);
            BufferedReader strCont = new BufferedReader(new InputStreamReader(processt.getInputStream()));
            String linet;
            StringBuffer sbt = new StringBuffer();
            while ((linet = strCont.readLine()) != null) {
                sbt.append(linet);
                sbt.append("\n");}
            strCont.close();
            processt.destroy();

            String urlxt = "\\src\\main\\java\\org\\example\\backend\\machinelearn\\way2.py";
            Process processtt = Runtime.getRuntime().exec(url1+url2+urlxt);
            BufferedReader strContt = new BufferedReader(new InputStreamReader(processtt.getInputStream()));
            String linett;
            StringBuffer sbtt = new StringBuffer();
            while ((linett = strContt.readLine()) != null) {
                sbtt.append(linett);
                sbtt.append("\n");}
            strContt.close();
            processt.destroy();



            String urlu = "\\src\\main\\java\\org\\example\\backend\\machinelearn\\way3.py";
            Process proc = Runtime.getRuntime().exec(url1+url2+urlu);
            BufferedReader strCo= new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String lin;
            StringBuffer sbc = new StringBuffer();
            while ((lin = strCo.readLine()) != null) {
                sbc.append(lin);
                sbc.append("\n");}
        strCo.close();
        proc.destroy();


        return "success";
    }




}
