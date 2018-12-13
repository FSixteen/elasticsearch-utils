package com.xyshzh.elasticsearch.connection;

import java.io.IOException;
import java.util.Map;
import java.util.Vector;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xyshzh.elasticsearch.ESConnection;

/**
 * @author Shengjun Liu
 * @date 2018-11-08
 */
public class RestClientConnection implements ESConnection<RestHighLevelClient> {
  private static final Logger log = LoggerFactory.getLogger(RestClientConnection.class);

  private String[] hosts = null;
  private Integer port = null;
  private Vector<HttpHost> addresses = new Vector<HttpHost>();
  private RestHighLevelClient client = null;

  public RestClientConnection(Map<String, Object> config) {
    try {
      log.info("ESConnection 初始化开始......");
      if (null == config || config.isEmpty()) throw new RuntimeException(this.getClass().getName() + " [config] is null or empty");
      this.hosts = config.containsKey("hosts") ? config.remove("hosts").toString().split(",") : new String[] { "127.0.0.1" };
      this.port = config.containsKey("port") ? Integer.valueOf(config.remove("port").toString()) : 9200;
      if (addresses.isEmpty()) {
        for (String ip : this.hosts) {
          if (1 == ip.split(":").length) {
            addresses.add(new HttpHost(ip, this.port, "http"));
          } else {
            String[] ip_port = ip.split(":");
            addresses.add(new HttpHost(ip_port[0], Integer.valueOf(ip_port[1]), "http"));
          }
        }
      }
      client = new RestHighLevelClient(RestClient.builder(addresses.toArray(new HttpHost[addresses.size()])));
      log.info("ESConnection 初始化结束......");
    } catch (Exception e) {
      e.printStackTrace();
      log.info("ESConnection 初始化异常......");
    }
  }

  @Override
  public RestHighLevelClient getClient() {
    return client;
  }

  @Override
  public void releaseClient(RestHighLevelClient client) {
    return;
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}