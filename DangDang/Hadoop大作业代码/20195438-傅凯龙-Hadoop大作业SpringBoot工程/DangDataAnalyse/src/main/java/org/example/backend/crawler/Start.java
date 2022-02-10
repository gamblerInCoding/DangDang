package org.example.backend.crawler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Timer;


@EnableScheduling
@Component
public class Start {
    @Autowired
    LogCollection logCollection;
    @Scheduled(fixedRate = 1000*450)
    public void taskOne(){
        logCollection.run();
    }
}
