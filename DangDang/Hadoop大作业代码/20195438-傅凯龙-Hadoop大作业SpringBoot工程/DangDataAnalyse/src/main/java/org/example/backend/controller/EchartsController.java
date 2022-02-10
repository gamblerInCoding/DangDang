package org.example.backend.controller;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.example.backend.entity.BookItem;
import org.example.backend.entity.PulishItem;
import org.example.backend.entity.QianDuan;
import org.example.backend.entity.ResultItem;
import org.example.backend.service.HBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class EchartsController {
    @Autowired
    HBaseService hBaseService;

    @RequestMapping("echarts1")
    public List<ArrayList> echarts1()throws IOException
    {
        List<ArrayList>books = hBaseService.lookAllprice();
        ArrayList<String>numbeers = new ArrayList<>();

        for(int i=500;i>=1;i--)
        {

            numbeers.add(String.valueOf(i));
        }
        books.add(numbeers);
        return books;
    }
    @RequestMapping("echarts1search")
    public List<ArrayList> echarts1search(@RequestBody Map<String,Object> map)throws IOException
    {
        List<ArrayList>books = hBaseService.looksearchprice(map);
        ArrayList<Integer>numbeers = new ArrayList<>();
        for(int i=500;i>=1;i--)
        {
            numbeers.add(i);
        }
        books.add(numbeers);
        return books;
    }



    @RequestMapping("allecharts1")
    public List<ArrayList> allecharts1()throws IOException
    {
        List<ArrayList>books = hBaseService.lookAlltheprice();
        ArrayList<String>numbeers = new ArrayList<>();
        for(int i=500;i>=1;i--)
        {
            numbeers.add(String.valueOf(i));
        }
        books.add(numbeers);
        System.out.println(books.toArray());
        return books;
    }
    @RequestMapping("chubanshe1")
    public List<QianDuan> chubanshe()throws IOException
    {
        List<PulishItem>models = hBaseService.getPulishResult();
        List<QianDuan>result = new ArrayList<>();
        for(PulishItem qian:models)
        {
            QianDuan qs = new QianDuan();
            qs.setName(qian.getPublisher());
            qs.setValue(Integer.parseInt(qian.getNumber()));
            result.add(qs);
        }
        return result;
    }
    @RequestMapping("chubanshe12")
    public List<QianDuan> chubanshe12(@RequestBody Map<String,Object> map)throws IOException
    {
        List<PulishItem>models = hBaseService.lookgetPulishResult(map);
        List<QianDuan>result = new ArrayList<>();
        for(PulishItem qian:models)
        {
            QianDuan qs = new QianDuan();
            qs.setName(qian.getPublisher());
            qs.setValue(Integer.parseInt(qian.getNumber()));
            result.add(qs);
        }
        return result;
    }
    @RequestMapping("leida12")
    public List<QianDuan> leida12()throws IOException
    {
        List<PulishItem>models = hBaseService.getPulishResult();
        List<QianDuan>result = new ArrayList<>();
        for(PulishItem qian:models)
        {
            QianDuan qs = new QianDuan();
            qs.setName(qian.getPublisher());
            qs.setValue(Integer.parseInt(qian.getNumber()));
            result.add(qs);
        }
        return result;
    }
    @RequestMapping("leida112")
    public List<QianDuan> leida112(@RequestBody Map<String,Object> map)throws IOException
    {
        List<PulishItem>models = hBaseService.lookgetPulishResult(map);
        List<QianDuan>result = new ArrayList<>();
        for(PulishItem qian:models)
        {
            QianDuan qs = new QianDuan();
            qs.setName(qian.getPublisher());
            qs.setValue(Integer.parseInt(qian.getNumber()));
            result.add(qs);
        }
        return result;
    }
}
