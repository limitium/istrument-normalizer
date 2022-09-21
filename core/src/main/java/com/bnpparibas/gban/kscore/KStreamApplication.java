package com.bnpparibas.gban.kscore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.bnpparibas.gban"})
public class KStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(KStreamApplication.class, args);
    }

}