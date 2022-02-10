package org.example.backend.service;
import org.example.backend.Dao.HBaseDao;
import org.example.backend.entity.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.springframework.web.bind.annotation.*;
import org.example.backend.service.HBaseService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
@Service
public class HBaseService {
    @Autowired
    HBaseDao hBaseDao;
    public List<PulishItem> getPulishResult()throws IOException{
        this.hBaseDao.init();
        List<PulishItem> xx = this.hBaseDao.scanPublishTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<ResultItem> getResult()throws IOException{
        this.hBaseDao.init();
        List<ResultItem> xx = this.hBaseDao.resultTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<BookItem> getBook1()throws IOException{
        this.hBaseDao.init();
        List<BookItem> xx = this.hBaseDao.scanTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<BookItem> getBook2()throws IOException{
        this.hBaseDao.init();
        List<BookItem> xx = this.hBaseDao.sclnTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<ResultItem> lookgetResult(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<ResultItem> xx = this.hBaseDao.lookresultTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<PulishItem> lookgetPulishResult(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<PulishItem> xx = this.hBaseDao.lookscanPublishTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<BookItem> lookgetBook1(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<BookItem> xx = this.hBaseDao.lookscanTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<ArrayList> lookAlltheprice() throws IOException {
        this.hBaseDao.init();
        List<ArrayList> xx = this.hBaseDao.lookAllTheprice();
        this.hBaseDao.close();
        return xx;
    }
    public List<ArrayList> lookAllprice() throws IOException {
        this.hBaseDao.init();
        List<ArrayList> xx = this.hBaseDao.lookAllprice();
        this.hBaseDao.close();
        return xx;
    }
    public List<ArrayList> looksearchprice(Map<String,Object> map) throws IOException {
        this.hBaseDao.init();
        List<ArrayList> xx = this.hBaseDao.looksearchprice(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<BookItem> lookgetBook2(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<BookItem> xx = this.hBaseDao.looksclnTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<FlowBeans> getTuijianResult()throws IOException{
        this.hBaseDao.init();
        List<FlowBeans> xx = this.hBaseDao.scanTuijianTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<FlowBeans> lookgetTuijianResult(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<FlowBeans> xx = this.hBaseDao.lookscanTuijianTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<FlowBeans> getCommentResult()throws IOException{
        this.hBaseDao.init();
        List<FlowBeans> xx = this.hBaseDao.scanCommentTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<FlowBeans> lookgetCommentResult(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<FlowBeans> xx = this.hBaseDao.lookscanCommentTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<PulishItem> getbaituijianResult()throws IOException{
        this.hBaseDao.init();
        List<PulishItem> xx = this.hBaseDao.scanbaituijianTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<PulishItem> lookgetbaituijianResult(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<PulishItem> xx = this.hBaseDao.lookscanbaituijianTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<BookItems2> getundertwojobResult()throws IOException{
        this.hBaseDao.init();
        List<BookItems2> xx = this.hBaseDao.scanundertwojobTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<BookItems2> lookundertwojobResult(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<BookItems2> xx = this.hBaseDao.lookundertwojobTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<CommentTuijian> scancommentdatazhishujobTable()throws IOException{
        this.hBaseDao.init();
        List<CommentTuijian> xx = this.hBaseDao.scancommentdatazhishujobTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<CommentTuijian> lookcommentdatazhishujobTable(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<CommentTuijian> xx = this.hBaseDao.lookcommentdatazhishujobTable(map);
        this.hBaseDao.close();
        return xx;
    }
    public List<PulishItem> getComResult()throws IOException{
        this.hBaseDao.init();
        List<PulishItem> xx = this.hBaseDao.scanComTable();
        this.hBaseDao.close();
        return xx;
    }
    public List<PulishItem> lookComResult(Map<String,Object> map)throws IOException{
        this.hBaseDao.init();
        List<PulishItem> xx = this.hBaseDao.lookComTable(map);
        this.hBaseDao.close();
        return xx;
    }


}
