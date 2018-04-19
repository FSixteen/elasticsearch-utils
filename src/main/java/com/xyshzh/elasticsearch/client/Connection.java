package com.xyshzh.elasticsearch.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.xyshzh.utils.Utils;

/**
 * @author Shengjun Liu
 * @version 2018-01-08
 */
public interface Connection {
  TransportClient getClient();

  void releaseClient(TransportClient client);

  void close();

  public final static class ClientConnection implements Connection {

    private Map<String, ?> config = null;
    private Builder builder = Settings.builder();
    private Settings settings = null;
    private Integer client_min_size = 10;
    private Integer client_max_size = client_min_size * 4;
    private Integer client_init_batch_size = client_min_size;
    private Vector<TransportClient> clients = new Vector<TransportClient>();

    public ClientConnection(Map<String, ?> config) {
      if (null == config || config.isEmpty())
        throw new RuntimeException(this.getClass().getName() + " [config] is null or empty");
      this.config = config;
      if (this.config.containsKey("cluster.name")) {
        builder.put("cluster.name", this.config.get("cluster.name"));
      }
      if (this.config.containsKey("client.transport.sniff")) {
        builder.put("client.transport.sniff", this.config.get("client.transport.sniff"));
      }
      settings = this.builder.build();
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

    @SuppressWarnings("resource")
    private void getClients() {
      synchronized (this.getClass()) {
        if (client_min_size > clients.size()) {
          for (int i = client_init_batch_size; i > 0; i--) {
            List<TransportAddress> addressList = getAllAddress();
            clients.addElement(new PreBuiltTransportClient(settings).addTransportAddresses(addressList.toArray(new TransportAddress[addressList.size()])));
          }
        }
      }
    }

    private List<TransportAddress> getAllAddress() {
      List<TransportAddress> addressList = new ArrayList<TransportAddress>();
      for (String ip : ((String) this.config.get("hosts")).split(",")) {
        if (!Utils.isStandardOfIPV4(ip)) {
          continue;
        } else {
          addressList.add(new TransportAddress(new InetSocketAddress(ip, (Integer) this.config.get("port"))));
        }
      }
      return addressList;
    }

    @Override
    public void close() {
      clients.forEach(_1 -> _1.close());
    }
  }
}
