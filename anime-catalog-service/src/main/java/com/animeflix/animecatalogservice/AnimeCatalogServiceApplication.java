package com.animeflix.animecatalogservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableReactiveMongoRepositories(basePackages = "com.animeflix.animecatalogservice.Repository")
public class AnimeCatalogServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnimeCatalogServiceApplication.class, args);
    }

}
