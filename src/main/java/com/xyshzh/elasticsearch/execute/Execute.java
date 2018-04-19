package com.xyshzh.elasticsearch.execute;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;

import com.xyshzh.elasticsearch.client.Connection;

/**
 * @author Shengjun Liu
 * @version 2018-01-08
 */
public class Execute {
  private Connection connection = null;

  public Execute(Connection connection) {
    this.connection = connection;
  }

  public Map<String, Object> get(String index, String type, String id) {
    TransportClient client = connection.getClient();
    Map<String, Object> hit = client.prepareGet(index, type, id).get().getSourceAsMap();
    connection.releaseClient(client);
    return hit;
  }

  public Long filter(List<Map<String, Object>> list, String index, String type, int from, int size,
      QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder) {
    return filter(list, new String[] { index }, new String[] { type }, from, size, null, null, queryBuilder, postFilter,
        sortBuilder);
  }

  public Long filter(List<Map<String, Object>> list, String[] index, String[] type, int from, int size,
      QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder) {
    return filter(list, index, type, from, size, null, null, queryBuilder, postFilter, sortBuilder);
  }

  public Long filter(List<Map<String, Object>> list, String index, String type, int from, int size, String[] includes,
      QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder) {
    return filter(list, new String[] { index }, new String[] { type }, from, size, includes, null, queryBuilder,
        postFilter, sortBuilder);
  }

  public Long filter(List<Map<String, Object>> list, String[] index, String[] type, int from, int size,
      String[] includes, QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder) {
    return filter(list, index, type, from, size, includes, null, queryBuilder, postFilter, sortBuilder);
  }

  public Long filter(List<Map<String, Object>> list, String index, String type, int from, int size, String[] includes,
      String[] excludes, QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder) {
    return filter(list, new String[] { index }, new String[] { type }, from, size, includes, excludes, queryBuilder,
        postFilter, sortBuilder);
  }

  public Long filter(List<Map<String, Object>> list, String[] index, String[] type, int from, int size,
      String[] includes, String[] excludes, QueryBuilder queryBuilder, QueryBuilder postFilter,
      SortBuilder<?> sortBuilder) {
    TransportClient client = connection.getClient();
    SearchRequestBuilder prepareSearch = client.prepareSearch().setIndices(index).setTypes(type);
    if (from < 0)
      throw new IllegalArgumentException("[from] parameter cannot be negative, found [" + from + "]");
    else
      prepareSearch.setFrom(from);
    if (size < 0)
      throw new IllegalArgumentException("[size] parameter cannot be negative, found [" + size + "]");
    else
      prepareSearch.setSize(size);
    prepareSearch.setFetchSource(includes, excludes);
    if (null != queryBuilder)
      prepareSearch.setQuery(queryBuilder);
    if (null != postFilter)
      prepareSearch.setPostFilter(postFilter);
    if (null != sortBuilder)
      prepareSearch.addSort(sortBuilder);
    System.out.println("ElasticSearch Query :: " + prepareSearch.toString());
    SearchHits searchHits = prepareSearch.get().getHits();
    searchHits.forEach(hits -> {
      Map<String, Object> hit = hits.getSourceAsMap();
      hit.put("_id", hits.getId());
      hit.put("_type", hits.getType());
      hit.put("_index", hits.getIndex());
      list.add(hit);
    });
    connection.releaseClient(client);
    return searchHits.getTotalHits();
  }

  public void add(String index, String type, String id, Map<String, Object> hit) {
    TransportClient client = connection.getClient();
    IndexRequestBuilder requestBuilder = client.prepareIndex(index, type, id).setSource(hit);
    requestBuilder.execute().actionGet();
    connection.releaseClient(client);
  }

  @SuppressWarnings("unchecked")
  public void add(String index, String type, String id, Map<String, Object> hit, Object... idHitPeers) {
    if ((idHitPeers.length % 2) != 0) {
      throw new IllegalArgumentException(
          "array idHitPeers of id + hit order doesn't hold correct number of arguments (" + idHitPeers.length + ")");
    }
    TransportClient client = connection.getClient();
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(client.prepareIndex(index, type, id).setSource(hit));
    for (int i = 0; i < idHitPeers.length - 1; i++) {
      bulkRequest.add(client.prepareIndex(index, type, String.class.cast(idHitPeers[i++]))
          .setSource(Map.class.cast(idHitPeers[i])));
    }
    bulkRequest.execute().actionGet();
    connection.releaseClient(client);
  }

  public void add(String index, String type, Map<?, ?>... hits) {
    TransportClient client = connection.getClient();
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    for (Map<?, ?> hit : hits) {
      bulkRequest.add(client.prepareIndex(index, type).setSource(hit));
    }
    bulkRequest.execute().actionGet();
    connection.releaseClient(client);
  }

}
