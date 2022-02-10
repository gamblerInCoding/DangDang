package org.example.backend.controller;
import java.io.IOException;

import org.example.backend.entity.*;
import org.springframework.web.bind.annotation.*;
import org.example.backend.service.HBaseService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
@RestController
public class HBaseController {
    @Autowired
    HBaseService hBaseService;
    @RequestMapping("booklist1")
    public List<BookItem> book1()throws IOException
    {
          return hBaseService.getBook1();

    }
    @RequestMapping("booklist2")
    public List<BookItem> book2()throws IOException
    {
        return hBaseService.getBook2();

    }

     @RequestMapping("result1")
    public List<ResultItem> result1()throws IOException
    {
        return hBaseService.getResult();
    }


    @RequestMapping("lookbooklist1")
    public List<BookItem> lookbook1(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookgetBook1(map);

    }
    @RequestMapping("lookbooklist2")
    public List<BookItem> lookbook2(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookgetBook2(map);

    }

    @RequestMapping("lookresult1")
    public List<ResultItem> lookresult1(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookgetResult(map);
    }


    @RequestMapping("lookpulishresult1")
    public List<PulishItem> lookpulishresult1(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookgetPulishResult(map);
    }



    @RequestMapping("pulishresult1")
    public List<PulishItem> pulishresult1()throws IOException
    {
        return hBaseService.getPulishResult();
    }
    @RequestMapping("looktuijianresult1")
    public List<FlowBeans> looktuijianresult1(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookgetTuijianResult(map);
    }



    @RequestMapping("tuijianresult1")
    public List<FlowBeans> tuijianresult1()throws IOException
    {
        return hBaseService.getTuijianResult();
    }

    @RequestMapping("lookcommentresult1")
    public List<FlowBeans> lookcommentresult1(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookgetCommentResult(map);
    }



    @RequestMapping("commentresult1")
    public List<FlowBeans> commentresult1()throws IOException
    {
        return hBaseService.getCommentResult();
    }

    @RequestMapping("lookbaituijianresult1")
    public List<PulishItem> lookbaituijianresult1(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookgetbaituijianResult(map);
    }



    @RequestMapping("baituijianresult1")
    public List<PulishItem> baituijianresult1()throws IOException
    {
        return hBaseService.getbaituijianResult();
    }


    @RequestMapping("lookundertwojobResult")
    public List<BookItems2> lookundertwojobResult(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookundertwojobResult(map);
    }



    @RequestMapping("getundertwojobResult")
    public List<BookItems2> getundertwojobResult()throws IOException
    {
        return hBaseService.getundertwojobResult();
    }


    @RequestMapping("lookcommentdatazhishujobTable")
    public List<CommentTuijian> lookcommentdatazhishujobTable(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookcommentdatazhishujobTable(map);
    }



    @RequestMapping("scancommentdatazhishujobTable")
    public List<CommentTuijian> scancommentdatazhishujobTable()throws IOException
    {
        return hBaseService.scancommentdatazhishujobTable();
    }
    @RequestMapping("lookComResult")
    public List<PulishItem> lookComResult(@RequestBody Map<String,Object> map)throws IOException
    {
        return hBaseService.lookComResult(map);
    }



    @RequestMapping("getComResult")
    public List<PulishItem> getComResult()throws IOException
    {
        return hBaseService.getComResult();
    }
}
