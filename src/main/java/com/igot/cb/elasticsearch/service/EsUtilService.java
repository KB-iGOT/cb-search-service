package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.util.ApiResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public interface EsUtilService {
  ApiResponse upsertTrendingSearch(String esIndexName, String id, Map<String, Object> document, String jsonFilePath);

  void deleteDocumentsByCriteria(String esIndexName, Query query);

  SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria) throws Exception;

  boolean isIndexPresent(String indexName);

  BulkResponse saveAll(String esIndexName, List<JsonNode> entities) throws IOException;

  Object readDocument(String esIndexName, String documentId) throws IOException;

}
