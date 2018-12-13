package com.xyshzh.elasticsearch.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xyshzh.elasticsearch.ESConnection;
import com.xyshzh.elasticsearch.ESExecutor;
import com.xyshzh.elasticsearch.connection.RestClientConnection;
import com.xyshzh.elasticsearch.domain.CollectionMapResult;

/**
 * @author Shengjun Liu
 * @date 2018-11-08
 */
public class RestClientExecutor implements ESExecutor {
  private static Logger log = LoggerFactory.getLogger(RestClientExecutor.class);

  private ESConnection<RestHighLevelClient> connection = null;

  public RestClientExecutor(Map<String, Object> config) {
    this.connection = new RestClientConnection(config);
  }

  public RestClientExecutor(String hosts, Integer port) {
    Map<String, Object> config = new HashMap<>();
    if (null != hosts) config.put("hosts", hosts);
    if (null != port) config.put("port", port);
    connection = new RestClientConnection(config);
  }

  @Override
  public boolean isIndexExist(String index) {
    try {
      log.info("获取索引: " + index + " 是否存在......");
      GetIndexRequest getIndexRequest = new GetIndexRequest().indices(index);
      boolean response = connection.getClient().indices().exists(getIndexRequest, RequestOptions.DEFAULT);
      return response;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public Map<String, Object> getRecord(String index, String type, String id, boolean incudeBaseContent) {
    try {
      GetRequest getRequest = new GetRequest(index, type, id);
      GetResponse response = connection.getClient().get(getRequest, RequestOptions.DEFAULT);
      if (response.isExists()) {
        Map<String, Object> hit = response.getSourceAsMap();
        if (incudeBaseContent && null != hit) {
          hit.put("_index", index);
          hit.put("_type", type);
          hit.put("_id", id);
        }
        return hit;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Collection<Map<String, Object>> getRecords(String index[], String type[], String[] id, boolean incudeBaseContent) {
    try {
      SearchRequest searchRequest = new SearchRequest(index).types(type);
      SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
      sourceBuilder.query(QueryBuilders.termsQuery("_id", id));
      sourceBuilder.from(0);
      sourceBuilder.size(id.length);
      searchRequest.source(sourceBuilder);
      log.info("ElasticSearch Query :: " + searchRequest.toString());
      SearchResponse searchResponse = connection.getClient().search(searchRequest, RequestOptions.DEFAULT);
      SearchHits searchHits = searchResponse.getHits();
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
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new ArrayList<>();
  }

  @Override
  public boolean deleteIndex(String index) {
    if (isIndexExist(index)) {
      DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
      try {
        AcknowledgedResponse response = connection.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        return response.isAcknowledged();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  @Override
  public boolean deleteRecord(String index, String type, String id) {
    try {
      DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
      DeleteResponse response = connection.getClient().delete(deleteRequest, RequestOptions.DEFAULT);
      return RestStatus.OK == response.status();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean createIndex(String index, String type, Map<String, ?> settings, XContentBuilder mapping) {
    if (!isIndexExist(index)) {
      try {
        CreateIndexRequest request = new CreateIndexRequest(index);
        if (null != type && null != mapping) request.mapping(type, mapping);
        if (null != settings) request.settings(settings);
        CreateIndexResponse createIndexResponse = connection.getClient().indices().create(request, RequestOptions.DEFAULT);
        return createIndexResponse.isAcknowledged();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  @Override
  public boolean insertRecord(String index, String type, String id, Map<String, Object> hit) {
    try {
      IndexRequest request = null == id ? new IndexRequest(index, type) : new IndexRequest(index, type, id);
      request.source(hit).opType(DocWriteRequest.OpType.INDEX);
      IndexResponse indexResponse = connection.getClient().index(request, RequestOptions.DEFAULT);
      return indexResponse.status() == RestStatus.CREATED || indexResponse.status() == RestStatus.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean insertRecords(String index, String type, String id1, Map<String, Object> hit1, String id2, Map<String, Object> hit2,
      Object... idHitPeers) {
    try {
      if ((idHitPeers.length % 2) != 0) { throw new IllegalArgumentException(
          "array idHitPeers of id + hit order doesn't hold correct number of arguments (" + idHitPeers.length + ")"); }
      BulkRequest bulkRequest = new BulkRequest();
      bulkRequest.add(new IndexRequest(index, type, id1).source(hit1).opType(DocWriteRequest.OpType.INDEX));
      bulkRequest.add(new IndexRequest(index, type, id2).source(hit2).opType(DocWriteRequest.OpType.INDEX));
      for (int i = 0; i < idHitPeers.length - 1; i++) {
        bulkRequest.add(new IndexRequest(index, type, String.class.cast(idHitPeers[i++])).source(Map.class.cast(idHitPeers[i]))
            .opType(DocWriteRequest.OpType.INDEX));
      }
      BulkResponse bulkResponse = connection.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
      return bulkResponse.status() == RestStatus.CREATED || bulkResponse.status() == RestStatus.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean insertRecords(String index, String type, Map<String, Object> hit1, Map<String, Object> hit2,
      @SuppressWarnings("unchecked") Map<String, Object>... hits) {
    try {
      BulkRequest bulkRequest = new BulkRequest();
      bulkRequest.add(new IndexRequest(index, type).source(hit1).opType(DocWriteRequest.OpType.INDEX));
      bulkRequest.add(new IndexRequest(index, type).source(hit2).opType(DocWriteRequest.OpType.INDEX));
      for (int i = 0; i < hits.length - 1; i++) {
        bulkRequest.add(new IndexRequest(index, type).source(hits[i]).opType(DocWriteRequest.OpType.INDEX));
      }
      BulkResponse bulkResponse = connection.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
      return bulkResponse.status() == RestStatus.CREATED || bulkResponse.status() == RestStatus.OK;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public CollectionMapResult filter(String[] index, String[] type, int from, int size, String[] includes, String[] excludes,
      QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder, boolean incudeBaseContent) {
    Long current = System.currentTimeMillis();
    try {
      SearchRequest searchRequest = new SearchRequest(index).types(type);
      SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
      if (from < 0) throw new IllegalArgumentException("[from] parameter cannot be negative, found [" + from + "]");
      else sourceBuilder.from(from);
      if (size < 0) throw new IllegalArgumentException("[size] parameter cannot be negative, found [" + size + "]");
      else sourceBuilder.size(size);
      sourceBuilder.fetchSource(includes, excludes);
      if (null != queryBuilder) sourceBuilder.query(queryBuilder);
      if (null != postFilter) sourceBuilder.postFilter(postFilter);
      if (null != sortBuilder) sourceBuilder.sort(sortBuilder);
      searchRequest.source(sourceBuilder);
      log.info("ElasticSearch Query :: " + searchRequest.toString());
      SearchResponse searchResponse = connection.getClient().search(searchRequest, RequestOptions.DEFAULT);
      SearchHits searchHits = searchResponse.getHits();
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
      log.info("ElasticSearch Execution Time :: " + (System.currentTimeMillis() - current) + " MS !");
      return new CollectionMapResult(searchHits.getTotalHits(), list);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new CollectionMapResult();
  }

  public void close() {
    this.connection.close();
  }
}