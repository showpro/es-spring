package com.zhan.logstash;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * springboot集成 log4j2 + logstash 异步输出日志
 */
@SpringBootApplication
public class LogstashApplication {

    private static final Logger logger = LogManager.getLogger(LogstashApplication.class);

    public static void main(String[] args) {

        SpringApplication.run(LogstashApplication.class, args);

        logger.info("springboot 集成 log4j2 + logstash!");

    }

}
