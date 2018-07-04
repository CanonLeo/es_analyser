package com.neusoft.hsyk;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.util.Properties;


/**
 * Created by ZhangQiang on 2018/6/6
 */

public class ElasticSearchUtil {

    private static Properties props;

    static {
        props = new Properties();
        try {
            props.load(ElasticSearchUtil.class.getClassLoader().getResourceAsStream("elasticsearch.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create ElasticSearch restful Client Builder
     *
     * @return
     */
    public static RestClientBuilder getRestClientBuilder() {

        String[] hosts = props.getProperty("hosts").split(",");
        int port = Integer.parseInt(props.getProperty("port"));
        String scheme = props.getProperty("scheme");

        HttpHost[] httpHosts = new HttpHost[hosts.length];

        for (int i = 0; i < hosts.length; i++) {
            httpHosts[i] = new HttpHost(hosts[i], port, scheme);
        }
        return RestClient.builder(httpHosts);
    }

}
