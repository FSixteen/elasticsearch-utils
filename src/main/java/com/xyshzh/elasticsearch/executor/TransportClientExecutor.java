package com.xyshzh.elasticsearch.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xyshzh.elasticsearch.ESConnection;
import com.xyshzh.elasticsearch.ESExecutor;
import com.xyshzh.elasticsearch.connection.TransportClientConnection;
import com.xyshzh.elasticsearch.domain.CollectionMapResult;

/**
 * @author Shengjun Liu
 * @date 2018-11-08
 */
public class TransportClientExecutor implements ESExecutor {
  private static Logger log = LoggerFactory.getLogger(TransportClientExecutor.class);

  private ESConnection<TransportClient> connection = null;

  public TransportClientExecutor(ESConnection<TransportClient> connection) {
    this.connection = connection;
  }

  public TransportClientExecutor(Map<String, Object> config) {
    this.connection = new TransportClientConnection(config);
  }

  public TransportClientExecutor(String hosts, Integer port) {
    Map<String, Object> config = new HashMap<>();
    if (null != hosts) config.put("hosts", hosts);
    if (null != port) config.put("port", 9300);
    config.put("cluster.name", "my-es");
    config.put("client.transport.ignore_cluster_name", false);
    config.put("client.transport.ping_timeout", "5s");
    config.put("client.transport.nodes_sampler_interval", "5s");
    config.put("client.transport.sniff", true);
    config.put("cluster.remote.connect", true);
    config.put("node.name", "haha");
    config.put("node.master", false);
    config.put("node.data", false);
    config.put("node.ingest", false);
    this.connection = new TransportClientConnection(config);
  }

  @Override
  public boolean isIndexExist(String index) {
    log.info("获取索引: " + index + " 是否存在......");
    TransportClient client = connection.getClient();
    IndicesExistsResponse response = client.admin().indices().prepareExists(index).execute().actionGet();
    connection.releaseClient(client);
    return response.isExists();
  }

  @Override
  public Map<String, Object> getRecord(String index, String type, String id, boolean incudeBaseContent) {
    TransportClient client = connection.getClient();
    GetResponse response = client.prepareGet(index, type, id).get();
    connection.releaseClient(client);
    if (response.isExists()) {
      Map<String, Object> hit = response.getSourceAsMap();
      if (incudeBaseContent && null != hit) {
        hit.put("_index", index);
        hit.put("_type", type);
        hit.put("_id", id);
      }
      return hit;
    }
    return null;
  }

  @Override
  public Collection<Map<String, Object>> getRecords(String index[], String type[], String[] id, boolean incudeBaseContent) {
    TransportClient client = connection.getClient();
    SearchRequestBuilder prepareSearch = client.prepareSearch().setIndices(index).setTypes(type);
    prepareSearch.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termsQuery("_id", id)));
    prepareSearch.setFrom(0);
    prepareSearch.setSize(id.length);
    log.info("ElasticSearch Query :: " + prepareSearch.toString());
    SearchResponse response = prepareSearch.get();
    connection.releaseClient(client);
    SearchHits searchHits = response.getHits();
    List<Map<String, Object>> list = new ArrayList<>();
    searchHits.forEach(hits -> {
      Map<String, Object> hit = hits.getSourceAsMap();
      if (incudeBaseContent && null != hit) {
        hit.put("_id", hits.getId());
        hit.put("_type", hits.getType());
        hit.put("_index", hits.getIndex());
      }
      list.add(hit);
    });
    return list;
  }

  @Override
  public boolean deleteIndex(String index) {
    TransportClient client = connection.getClient();
    boolean status = isIndexExist(index) ? client.admin().indices().prepareDelete(index).execute().actionGet().isAcknowledged() : false;
    connection.releaseClient(client);
    return status;
  }

  @Override
  public boolean deleteRecord(String index, String type, String id) {
    TransportClient client = connection.getClient();
    DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
    connection.releaseClient(client);
    return RestStatus.OK == response.status();
  }

  @Override
  public boolean createIndex(String index, String type, Map<String, ?> settings, XContentBuilder mapping) {
    if (!isIndexExist(index)) {
      TransportClient client = connection.getClient();
      CreateIndexRequestBuilder cib = client.admin().indices().prepareCreate(index);
      if (null != type && null != mapping) cib.addMapping(type, mapping);
      if (null != settings) cib.setSettings(settings);
      boolean status = cib.execute().actionGet().isAcknowledged();
      connection.releaseClient(client);
      return status;
    }
    return false;
  }

  @Override
  public boolean insertRecord(String index, String type, String id, Map<String, Object> hit) {
    TransportClient client = connection.getClient();
    IndexRequestBuilder requestBuilder = null == id ? client.prepareIndex(index, type) : client.prepareIndex(index, type, id);
    requestBuilder.setSource(hit).setOpType(DocWriteRequest.OpType.INDEX);
    IndexResponse response = requestBuilder.execute().actionGet();
    connection.releaseClient(client);
    return response.status() == RestStatus.CREATED || response.status() == RestStatus.OK;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean insertRecords(String index, String type, String id1, Map<String, Object> hit1, String id2, Map<String, Object> hit2,
      Object... idHitPeers) {
    if ((idHitPeers.length % 2) != 0) { throw new IllegalArgumentException(
        "array idHitPeers of id + hit order doesn't hold correct number of arguments (" + idHitPeers.length + ")"); }
    TransportClient client = connection.getClient();
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(client.prepareIndex(index, type, id1).setSource(hit1));
    bulkRequest.add(client.prepareIndex(index, type, id2).setSource(hit2));
    for (int i = 0; i < idHitPeers.length - 1; i++) {
      bulkRequest.add(client.prepareIndex(index, type, String.class.cast(idHitPeers[i++])).setSource(Map.class.cast(idHitPeers[i])));
    }
    BulkResponse response = bulkRequest.execute().actionGet();
    connection.releaseClient(client);
    return response.status() == RestStatus.CREATED || response.status() == RestStatus.OK;
  }

  @Override
  public boolean insertRecords(String index, String type, Map<String, Object> hit1, Map<String, Object> hit2,
      @SuppressWarnings("unchecked") Map<String, Object>... hits) {
    TransportClient client = connection.getClient();
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    bulkRequest.add(client.prepareIndex(index, type).setSource(hit1));
    bulkRequest.add(client.prepareIndex(index, type).setSource(hit2));
    for (int i = 0; i < hits.length; i++) {
      bulkRequest.add(client.prepareIndex(index, type).setSource(hits[i]));
    }
    BulkResponse response = bulkRequest.execute().actionGet();
    connection.releaseClient(client);
    return response.status() == RestStatus.CREATED || response.status() == RestStatus.OK;
  }

  @Override
  public CollectionMapResult filter(String[] index, String[] type, int from, int size, String[] includes, String[] excludes,
      QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder, boolean incudeBaseContent) {
    Long current = System.currentTimeMillis();
    TransportClient client = connection.getClient();
    SearchRequestBuilder prepareSearch = client.prepareSearch().setIndices(index).setTypes(type);
    if (from < 0) throw new IllegalArgumentException("[from] parameter cannot be negative, found [" + from + "]");
    else prepareSearch.setFrom(from);
    if (size < 0) throw new IllegalArgumentException("[size] parameter cannot be negative, found [" + size + "]");
    else prepareSearch.setSize(size);
    prepareSearch.setFetchSource(includes, excludes);
    if (null != queryBuilder) prepareSearch.setQuery(queryBuilder);
    if (null != postFilter) prepareSearch.setPostFilter(postFilter);
    if (null != sortBuilder) prepareSearch.addSort(sortBuilder);
    log.info("ElasticSearch Query :: " + prepareSearch.toString());
    SearchHits searchHits = prepareSearch.get().getHits();
    List<Map<String, Object>> list = new ArrayList<>();
    searchHits.forEach(hits -> {
      Map<String, Object> hit = hits.getSourceAsMap();
      hit.put("_id", hits.getId());
      hit.put("_type", hits.getType());
      hit.put("_index", hits.getIndex());
      list.add(hit);
    });
    connection.releaseClient(client);
    log.info("ElasticSearch Execution Time :: " + (System.currentTimeMillis() - current) + " MS !");
    return new CollectionMapResult(searchHits.getTotalHits(), list);
  }

  public void close() {
    this.connection.close();
  }

}
