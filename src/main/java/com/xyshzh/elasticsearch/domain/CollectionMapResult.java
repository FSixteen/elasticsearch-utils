package com.xyshzh.elasticsearch.domain;

import java.util.Collection;
import java.util.Map;

/**
 * @author Shengjun Liu
 * @date 2018-11-08
 */
public class CollectionMapResult implements ESResult<Collection<Map<String, Object>>, Map<String, Object>> {

  private long totalHits = 0l;

  private Collection<Map<String, Object>> hits = null;

  public CollectionMapResult() {}

  public CollectionMapResult(long totalHits, Collection<Map<String, Object>> hits) {
    this.totalHits = totalHits;
    this.hits = hits;
  }

  public CollectionMapResult setTotalHits(long totalHits) {
    this.totalHits = totalHits;
    return this;
  }

  public CollectionMapResult setHits(Collection<Map<String, Object>> hits) {
    this.hits = hits;
    return this;
  }

  @Override
  public long totalHits() {
    return totalHits;
  }

  @Override
  public Collection<Map<String, Object>> currentHits() {
    return hits;
  }

  @Override
  public String toString() {
    return "CollectionMapResult:['totalHits':'" + this.totalHits + "', 'hits':'" + this.hits + "']";
  }

}
