package com.animeflix.animeepisode.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class ExternalApiConfig {

    @Value("${consumet.uri}")
    private String consumetUri;

    @Value("${malsync.uri}")
    private String malsyncUri;

    @Value("${zoro.uri}")
    private String zoroUri;

    @Value("${gogo.uri}")
    private String gogoUri;

    @Value("${anify.url}")
    private String anifyUrl;

    @Value("${animepahe.url}")
    private String animepaheUrl;

    @Value("${animapping.url}")
    private String animappingUrl;

    @Value("${anify.schedule.url}")
    private String anifyScheduleUrl;

    @Bean
    public WebClient consumetWebClient() {
        return WebClient.builder().baseUrl(consumetUri).build();
    }

    @Bean
    public WebClient malsyncWebClient() {
        return WebClient.builder().baseUrl(malsyncUri).build();
    }

    @Bean
    public WebClient zoroWebClient() {
        return WebClient.builder().baseUrl(zoroUri).build();
    }

    @Bean
    public WebClient gogoWebClient() {
        return WebClient.builder().baseUrl(gogoUri).build();
    }


    @Bean
    public WebClient anifyWebClient() {
        return WebClient.builder().baseUrl(anifyUrl).build();
    }

    @Bean
    public WebClient animepaheWebClient() {
        return WebClient.builder().baseUrl(animepaheUrl.replace("{anime_session}/{session}", "")).build();  // Adjust dynamic
    }

    @Bean
    public WebClient animappingWebClient() {
        return WebClient.builder().baseUrl(animappingUrl.split("\\?")[0]).build();  // Base without query
    }

    @Bean
    public WebClient anifyScheduleWebClient() {
        return WebClient.builder().baseUrl(anifyScheduleUrl).build();
    }
}