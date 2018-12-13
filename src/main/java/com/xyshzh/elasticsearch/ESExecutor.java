package com.xyshzh.elasticsearch;

import java.util.Collection;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import com.xyshzh.elasticsearch.domain.CollectionMapResult;

/**
 * @author Shengjun Liu
 * @date 2018-01-08
 */
public interface ESExecutor {
  /**
   * 获取索引是否存在.
   * @param index 索引.
   * @return
   */
  boolean isIndexExist(String index);

  /**
   * 获取索引类型是否存在.
   * @param index 索引.
   * @param type 类型.
   * @return
   */
  default boolean isTypeExist(String index, String type) {
    return false;
  }

  /**
   * 获取指定索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param id 索引ID.
   * @return
   */
  default Map<String, Object> getRecord(String index, String type, String id) {
    return getRecord(index, type, id, false);
  }

  /**
   * 获取指定索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param id 索引ID.
   * @param incudeBaseContent 包含基本信息.
   * @return
   */
  Map<String, Object> getRecord(String index, String type, String id, boolean incudeBaseContent);

  /**
   * 获取指定索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param id 索引ID.
   * @param incudeBaseContent 包含基本信息.
   * @return
   */
  default Collection<Map<String, Object>> getRecords(String index, String type, String[] id, boolean incudeBaseContent) {
    return getRecords(new String[] { index }, new String[] { type }, id, incudeBaseContent);
  }

  /**
   * 获取指定索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param id 索引ID.
   * @param incudeBaseContent 包含基本信息.
   * @return
   */
  Collection<Map<String, Object>> getRecords(String index[], String type[], String[] id, boolean incudeBaseContent);

  /**
   * 删除指定索引.
   * @param index 索引.
   * @return
   */
  boolean deleteIndex(String index);

  /**
   * 删除指定索引类型.
   * @param index 索引.
   * @param type 类型.
   * @return
   */
  default boolean deleteType(String index, String type) {
    return false;
  }

  /**
   * 删除指定索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param id 索引ID.
   * @return
   */
  boolean deleteRecord(String index, String type, String id);

  /**
   * 创建索引信息.
   * @param index 索引.
   * @return
   */
  default boolean createIndex(String index) {
    return createIndex(index, null);
  }

  /**
   * 创建索引信息.
   * @param index 索引.
   * @param type 类型.
   * @return
   */
  default boolean createIndex(String index, String type) {
    return createIndex(index, type, null, null);
  }

  /**
   * 创建索引信息.
   * @param index 索引.
   * @param type 类型.
   * @param mapping 属性信息. 
   * @return
   */
  default boolean createIndex(String index, String type, XContentBuilder mapping) {
    return createIndex(index, type, null, mapping);
  }

  /**
   * 创建索引信息.
   * @param index 索引.
   * @param type 类型.
   * @param settings 配置信息.
   * @return
   */
  default boolean createIndex(String index, String type, Map<String, ?> settings) {
    return createIndex(index, type, settings, null);
  }

  /**
   * 创建索引信息.
   * @param index 索引.
   * @param type 类型.
   * @param settings 配置信息.
   * @param mapping 属性信息. 
   * @return
   */
  boolean createIndex(String index, String type, Map<String, ?> settings, XContentBuilder mapping);

  /**
   * 添加索引记录.
   * @param index 索引.
   * @param type 类型.
   * @param id 索引ID.
   * @return
   */
  default boolean insertRecord(String index, String type, Map<String, Object> hit) {
    return insertRecord(index, type, null, hit);
  }

  /**
   * 添加索引记录.
   * @param index 索引.
   * @param type 类型.
   * @param id 索引ID.
   * @param hit 索引内容.
   * @return
   */
  boolean insertRecord(String index, String type, String id, Map<String, Object> hit);

  /**
   * 添加索引记录.
   * @param index 索引.
   * @param type 类型.
   * @param id1 索引ID.
   * @param hit1 索引内容.
   * @param id2 索引ID.
   * @param hit2 索引内容.
   * @param idHitPeers 索引ID+索引内容.
   * @return
   */
  boolean insertRecords(String index, String type, String id1, Map<String, Object> hit1, String id2, Map<String, Object> hit2,
      Object... idHitPeers);

  /**
   * 添加索引记录.
   * @param index 索引.
   * @param type 类型.
   * @param hit1 索引内容.
   * @param hit2 索引内容.
   * @param hits 索引内容.
   * @return
   */
  boolean insertRecords(String index, String type, Map<String, Object> hit1, Map<String, Object> hit2,
      @SuppressWarnings("unchecked") Map<String, Object>... hits);

  /**
   * 获取筛选索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param from 查询起始位置.
   * @param size 查询数量.
   * @param queryBuilder 参与计算的查询条件.
   * @param sortBuilder 排序.
   * @param incudeBaseContent 包含基本信息.
   * @return
   */
  default CollectionMapResult filter(String index, String type, int from, int size, QueryBuilder queryBuilder, SortBuilder<?> sortBuilder,
      boolean incudeBaseContent) {
    return filter(new String[] { index }, new String[] { type }, from, size, null, null, queryBuilder, null, sortBuilder,
        incudeBaseContent);
  }

  /**
   * 获取筛选索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param from 查询起始位置.
   * @param size 查询数量.
   * @param queryBuilder 参与计算的查询条件.
   * @param postFilter 参数筛选的查询条件.
   * @param sortBuilder 排序.
   * @param incudeBaseContent 包含基本信息.
   * @return
   */
  default CollectionMapResult filter(String index, String type, int from, int size, QueryBuilder queryBuilder, QueryBuilder postFilter,
      SortBuilder<?> sortBuilder, boolean incudeBaseContent) {
    return filter(new String[] { index }, new String[] { type }, from, size, null, null, queryBuilder, postFilter, sortBuilder,
        incudeBaseContent);
  }

  /**
   * 获取筛选索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param from 查询起始位置.
   * @param size 查询数量.
   * @param includes 包含的字段.
   * @param excludes 排除的字段.
   * @param queryBuilder 参与计算的查询条件.
   * @param postFilter 参数筛选的查询条件.
   * @param sortBuilder 排序.
   * @param incudeBaseContent 包含基本信息.
   * @return
   */
  default CollectionMapResult filter(String index, String type, int from, int size, String[] includes, String[] excludes,
      QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder, boolean incudeBaseContent) {
    return filter(new String[] { index }, new String[] { type }, from, size, includes, excludes, queryBuilder, postFilter, sortBuilder,
        incudeBaseContent);
  }

  /**
   * 获取筛选索引内容.
   * @param index 索引.
   * @param type 类型.
   * @param from 查询起始位置.
   * @param size 查询数量.
   * @param includes 包含的字段.
   * @param excludes 排除的字段.
   * @param queryBuilder 参与计算的查询条件.
   * @param postFilter 参数筛选的查询条件.
   * @param sortBuilder 排序.
   * @param incudeBaseContent 包含基本信息.
   * @return
   */
  CollectionMapResult filter(String[] index, String[] type, int from, int size, String[] includes, String[] excludes,
      QueryBuilder queryBuilder, QueryBuilder postFilter, SortBuilder<?> sortBuilder, boolean incudeBaseContent);

  /**
   * 释放资源.
   */
  void close();
}
