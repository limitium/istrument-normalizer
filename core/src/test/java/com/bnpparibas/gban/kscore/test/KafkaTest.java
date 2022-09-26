package com.bnpparibas.gban.kscore.test;

import com.bnpparibas.gban.kscore.KStreamApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})

@TestPropertySource(properties = {"kafka.bootstrap.servers=localhost:9092"})
@TestExecutionListeners(listeners = {DependencyInjectionTestExecutionListener.class, BaseKStreamApplicationTests.CustomExecutionListener.class})
public @interface KafkaTest {
    @AliasFor(annotation = EmbeddedKafka.class, attribute = "topics")
    String[] topics() default "";

    String[] consumers() default "";

    @AliasFor(annotation = SpringBootTest.class, attribute = "classes")
    Class<?>[] configs() default {KStreamApplication.class, BaseKStreamApplicationTests.BaseKafkaTestConfig.class};
}