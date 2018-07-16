package com.xyshzh.elasticsearch.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xyshzh.elasticsearch.client.Connection;
import com.xyshzh.elasticsearch.execute.Execute;

public class Test {
  @org.junit.Test
  public void test(String[] args) {
    Map<String, Object> config = new HashMap<>();
    config.put("hosts", "20.28.30.22");
    config.put("port", 9300);
    config.put("cluster.name", "my-es");
    config.put("client.transport.sniff", true);
    Execute execute = new Execute(new Connection.ClientConnection(config));

    List<Map<String, Object>> list = new ArrayList<>();
    execute.filter(list, "my_index", "a", 0, 10, new String[] { "id", "uid", "name" }, null, null, null);
    System.out.println(list.size());
    list.forEach(m -> {
      System.out.println("----------------------");
      m.forEach((k, v) -> {
        System.out.println(k + "   :::   " + v);
      });
    });
  }
}
