package org.example.backend;

import org.example.backend.crawler.LogCollection;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Timer;

@SpringBootApplication
public class ApplicationStartup
{
       public static void main(String[] args) {
           SpringApplication.run(ApplicationStartup.class, args);

            }
}