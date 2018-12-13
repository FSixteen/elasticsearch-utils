package com.xyshzh.elasticsearch;

/**
 * @author Shengjun Liu
 * @date 2018-01-08
 */
public interface ESConnection<T> {
  T getClient();

  void releaseClient(T client);

  void close();

}
