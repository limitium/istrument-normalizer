package com.bnpparibas.gban.kscore.test;

import com.bnpparibas.gban.kscore.KStreamApplication;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@SpringBootTest
@EmbeddedKafka(brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
@TestPropertySource(properties = {"kafka.bootstrap.servers=localhost:9092"})
@TestExecutionListeners(
        listeners = {
                DependencyInjectionTestExecutionListener.class,
                BaseKStreamApplicationTests.CustomExecutionListener.class
        })
public @interface KafkaTest {
    @AliasFor(annotation = EmbeddedKafka.class, attribute = "topics")
    String[] topics() default "";

    @AliasFor(annotation = EmbeddedKafka.class, attribute = "partitions")
    int partitions() default 2;

    String[] consumers() default "";

    @AliasFor(annotation = SpringBootTest.class, attribute = "classes")
    Class<?>[] configs() default {
            KStreamApplication.class, BaseKStreamApplicationTests.BaseKafkaTestConfig.class
    };
}
