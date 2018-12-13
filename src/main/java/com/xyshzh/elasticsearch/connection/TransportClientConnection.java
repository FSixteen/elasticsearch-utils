package com.xyshzh.elasticsearch.connection;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Vector;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xyshzh.elasticsearch.ESConnection;

/**
 * @author Shengjun Liu
 * @date 2018-11-08
 */
public class TransportClientConnection implements ESConnection<TransportClient> {
  private static final Logger log = LoggerFactory.getLogger(TransportClientConnection.class);

  private Builder builder = Settings.builder();
  private Settings settings = null;
  private Integer client_min_size = 10;
  private Integer client_max_size = client_min_size * 4;
  private Integer client_init_batch_size = client_min_size;
  private String[] hosts = null;
  private Integer port = null;
  private Vector<TransportAddress> addresses = new Vector<TransportAddress>();
  private Vector<TransportClient> clients = new Vector<TransportClient>();

  public TransportClientConnection(Map<String, Object> config) {
    try {
      log.info("ESConnection 初始化开始......");
      if (null == config || config.isEmpty()) throw new RuntimeException(this.getClass().getName() + " [config] is null or empty");
      this.hosts = config.containsKey("hosts") ? config.remove("hosts").toString().split(",") : new String[] { "127.0.0.1" };
      this.port = config.containsKey("port") ? Integer.valueOf(config.remove("port").toString()) : 9300;
      config.forEach((k, v) -> {
        switch (k) {
        case "cluster.name": // 集群名称.
        case "client.transport.ignore_cluster_name": // Set to true to ignore cluster name validation of connected nodes. (since 0.19.4).
        case "client.transport.ping_timeout": // The time to wait for a ping response from a node. Defaults to 5s.
        case "client.transport.nodes_sampler_interval": // How often to sample / ping the nodes listed and connected. Defaults to 5s.
        case "client.transport.sniff": // 嗅探.
        case "cluster.remote.connect":
        case "node.name":
        case "node.master":
        case "node.data":
        case "node.ingest":
          if (v instanceof Integer) {
            builder.put(k, Integer.valueOf(v.toString()));
          } else if (v instanceof Boolean) {
            builder.put(k, Boolean.valueOf(v.toString()));
          } else {
            builder.put(k, v.toString());
          }
          break;
        default:
          break;
        }
      });
      settings = this.builder.build();
      log.info("ESConnection 初始化结束......");
    } catch (Exception e) {
      e.printStackTrace();
      log.info("ESConnection 初始化异常......");
    }
  }

  @Override
  public TransportClient getClient() {
    if (0 < clients.size()) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          getClients();
        }
      }).start();
      return clients.remove(0);
    } else {
      synchronized (String.class) {
        if (0 < clients.size()) {
          return clients.remove(0);
        } else {
          getClients();
          return clients.remove(0);
        }
      }
    }
  }

  @Override
  public void releaseClient(TransportClient client) {
    if (null == client) {
      return;
    } else if (client_max_size < clients.size()) {
      client.close();
    } else {
      clients.addElement(client);
    }
  }

  private void getClients() {
    synchronized (this.getClass()) {
      if (client_min_size > clients.size()) {
        for (int i = client_init_batch_size; i > 0; i--) {
          PreBuiltTransportClient preTransportClient = new PreBuiltTransportClient(settings);
          if (addresses.isEmpty()) {
            for (String ip : hosts) {
              if (1 == ip.split(":").length) {
                addresses.add(new TransportAddress(new InetSocketAddress(ip, port)));
              } else {
                String[] ip_port = ip.split(":");
                addresses.add(new TransportAddress(new InetSocketAddress(ip_port[0], Integer.valueOf(ip_port[1]))));
              }
            }
          }
          preTransportClient.addTransportAddresses(addresses.toArray(new TransportAddress[addresses.size()]));
          clients.addElement(preTransportClient);
        }
      }
    }
  }

  @Override
  public void close() {
    clients.forEach(c -> c.close());
  }
}