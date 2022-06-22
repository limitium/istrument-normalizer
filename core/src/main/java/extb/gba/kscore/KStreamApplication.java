package extb.gba.kscore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"extb.gba"})
public class KStreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(KStreamApplication.class, args);
    }

}
