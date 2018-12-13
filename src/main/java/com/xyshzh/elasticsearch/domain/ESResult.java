package com.xyshzh.elasticsearch.domain;

/**
 * @author Shengjun Liu
 * @date 2018-11-08
 */
public interface ESResult<C, E> {
  long totalHits();

  C currentHits();

}
