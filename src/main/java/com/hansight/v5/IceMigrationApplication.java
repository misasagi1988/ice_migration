package com.hansight.v5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IceMigrationApplication {
    private final static Logger LOG = LoggerFactory.getLogger(IceMigrationApplication.class);
	public static void main(String[] args) {
        LOG.info("start migration service...");
		SpringApplication.run(IceMigrationApplication.class, args);
	}
}
