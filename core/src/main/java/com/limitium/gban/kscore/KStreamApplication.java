package com.limitium.gban.kscore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.limitium.gban"})
public class KStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(KStreamApplication.class, args);
    }

}