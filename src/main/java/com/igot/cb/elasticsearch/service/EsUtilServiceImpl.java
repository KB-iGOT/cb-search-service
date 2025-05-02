package com.igot.cb.elasticsearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.util.ApiResponse;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.elasticsearch.config.EsConfig;
import com.igot.cb.elasticsearch.dto.FacetDTO;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.exceptions.CustomException;
import com.igot.cb.util.ProjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EsUtilServiceImpl implements EsUtilService {


    private final ElasticsearchClient elasticsearchClient;
    private final ObjectMapper objectMapper;
    private final Set<String> NON_TEXT_FIELDS;
    private final RestClient restClient;

    @Autowired
    public EsUtilServiceImpl(ElasticsearchClient elasticsearchClient,
                             EsConfig esConnection,
                             ObjectMapper objectMapper,
                             CbServerProperties cbServerProperties, RestClient restClient) {
        this.elasticsearchClient = elasticsearchClient;
        this.objectMapper = objectMapper;

        this.NON_TEXT_FIELDS = Arrays.stream(cbServerProperties.getNonTextFields().split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
        this.restClient = restClient;
    }




    @Override
    public ApiResponse upsertTrendingSearch(
            String esIndexName, String id, Map<String, Object> document, String jsonFilePath) {
        log.info("EsUtilServiceImpl :: upsertTrendingSearch");
        ApiResponse response = ProjectUtil.createDefaultResponse("upsertTrendingSearch");
        try {
            String dateString = document.getOrDefault("last_searched", Instant.now().toString()).toString();
            Instant instant = Instant.parse(dateString);
            String esFormattedDate = instant.atZone(ZoneId.of("Asia/Kolkata"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
            document.put("last_searched", esFormattedDate);
            document.putIfAbsent("search_count", 1);
            String scriptSource = """
                    {
                      "script": {
                        "source": "if (ctx._source.search_count == null) { ctx._source.search_count = 1; } else { ctx._source.search_count += 1; } ctx._source.last_searched = params.date;",
                        "params": { "date": "%s" }
                      },
                      "upsert": %s
                    }
                    """.formatted(esFormattedDate, objectMapper.writeValueAsString(document));

            Request request = new Request("POST", "/" + esIndexName + "/_doc/" + id + "/_update?refresh=true");
            request.setJsonEntity(scriptSource);
            Response esResponse = restClient.performRequest(request);
            String status = esResponse.getStatusLine().getReasonPhrase();
            if ("OK".equalsIgnoreCase(status)||"Created".equalsIgnoreCase(status)) {
                response.getParams().setStatus(Constants.SUCCESS);
            } else {
                response.getParams().setErrMsg("Failed to update Elasticsearch document");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return response;
        } catch (Exception e) {
            log.error("Error updating Elasticsearch document", e);
            response.getParams().setErrMsg(e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }


    @Override
    public SearchResult searchDocuments(String esIndexName, SearchCriteria searchCriteria) {
        SearchRequest.Builder searchRequestBuilder = buildSearchRequest(searchCriteria);
        assert searchRequestBuilder != null;
        searchRequestBuilder.index(esIndexName);
        try {
            if (searchCriteria != null) {
                int pageNumber = searchCriteria.getPageNumber();
                int pageSize = searchCriteria.getPageSize();
                int from = pageNumber * pageSize;
                searchRequestBuilder.from(from);
                if (pageSize > 0) {
                    searchRequestBuilder.size(pageSize);
                }
            }
            SearchRequest searchRequest = searchRequestBuilder.build();
            log.info("Final search query: {}", searchRequest.toString());
            SearchResponse<Object> paginatedSearchResponse =
                    elasticsearchClient.search(searchRequest, Object.class);
            List<Map<String, Object>> paginatedResult = extractPaginatedResult(paginatedSearchResponse);
            Map<String, List<FacetDTO>> fieldAggregations =
                    extractFacetData(paginatedSearchResponse, searchCriteria);
            SearchResult searchResult = new SearchResult();
            searchResult.setData(objectMapper.valueToTree(paginatedResult));
            searchResult.setFacets(fieldAggregations);
            searchResult.setTotalCount(paginatedSearchResponse.hits().total().value());
            return searchResult;
        } catch (IOException e) {
            log.error("Error while fetching details from elastic search");
            return null;
        }
    }

    private Map<String, List<FacetDTO>> extractFacetData(
            SearchResponse<Object> searchResponse, SearchCriteria searchCriteria) {
        Map<String, List<FacetDTO>> fieldAggregations = new HashMap<>();
        if (searchCriteria.getFacets() != null) {
            for (String field : searchCriteria.getFacets()) {
                Aggregate aggregate = searchResponse
                        .aggregations()
                        .get(field + "_agg");

                List<FacetDTO> fieldValueList = new ArrayList<>();
                if (aggregate.isSterms()) {
                    for (StringTermsBucket bucket : aggregate.sterms().buckets().array()) {
                        if (!bucket.key().stringValue().isEmpty()) {
                            fieldValueList.add(new FacetDTO(bucket.key().stringValue(), bucket.docCount()));
                        }
                    }
                } else if (aggregate.isLterms()) { // for long/integer fields
                    for (LongTermsBucket bucket : aggregate.lterms().buckets().array()) {
                        fieldValueList.add(new FacetDTO(String.valueOf(bucket.key()), bucket.docCount()));
                    }
                } else if (aggregate.isDterms()) { // for double/float fields
                    for (DoubleTermsBucket bucket : aggregate.dterms().buckets().array()) {
                        fieldValueList.add(new FacetDTO(String.valueOf(bucket.key()), bucket.docCount()));
                    }
                }

                if (!fieldValueList.isEmpty()) {
                    fieldAggregations.put(field, fieldValueList);
                }
            }
        }
        return fieldAggregations;
    }

    private List<Map<String, Object>> extractPaginatedResult(SearchResponse<Object> paginatedSearchResponse) {
        List<Map<String, Object>> paginatedResult = new ArrayList<>();
        for (Hit<Object> hit : paginatedSearchResponse.hits().hits()) {
            paginatedResult.add((Map<String, Object>) hit.source());
        }
        return paginatedResult;
    }

    private SearchRequest.Builder buildSearchRequest(SearchCriteria searchCriteria) {
        log.info("Building search query");
        if (searchCriteria == null || searchCriteria.toString().isEmpty()) {
            log.error("Search criteria body is missing");
            return null;
        }
        BoolQuery.Builder boolQueryBuilder = buildFilterQuery(searchCriteria.getFilterCriteriaMap());
        SearchRequest.Builder searchSourceBuilder = new SearchRequest.Builder();
        searchSourceBuilder.query(boolQueryBuilder.build()._toQuery());
        addSortToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
        addRequestedFieldsToSearchSourceBuilder(searchCriteria, searchSourceBuilder);
        addQueryStringToFilter(searchCriteria.getSearchString(), boolQueryBuilder);
        addFacetsToSearchSourceBuilder(searchCriteria.getFacets(), searchSourceBuilder);
        Query queryPart = buildQueryPart(searchCriteria.getQuery());
        boolQueryBuilder.must(queryPart);
        return searchSourceBuilder;
    }

    private BoolQuery.Builder buildFilterQuery(Map<String, Object> filterCriteriaMap) {
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        List<Query> mustNotQueries = new ArrayList<>();
        List<Query> boolQueries = new ArrayList<>();

        if (filterCriteriaMap != null) {
            filterCriteriaMap.forEach(
                    (field, value) -> {
                        String actualField = NON_TEXT_FIELDS.contains(field) ? field + Constants.KEYWORD : field;

                        if (field.equals("must_not") && value instanceof ArrayList) {
                            mustNotQueries.add(Query.of(q -> q.termsSet(t -> t.field(actualField).terms((ArrayList<String>) value))));
                        } else if (value instanceof Boolean) {
                            boolQueries.add(Query.of(q -> q.term(t -> t.field(actualField).value((boolean) value))));
                        } else if (value instanceof ArrayList) {
                            List<FieldValue> termsList = ((ArrayList<String>) value).stream()
                                    .map(FieldValue::of)
                                    .collect(Collectors.toList());
                            boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(actualField).terms(terms -> terms.value(termsList)))));
                        } else if (value instanceof String) {
                            boolQueryBuilder.must(Query.of(q -> q.term(t ->
                                    t.field(actualField)
                                            .value(FieldValue.of((String) value)))));
                        } else if (value instanceof Integer) {
                            boolQueryBuilder.must(Query.of(q -> q.term(t ->
                                    t.field(actualField)
                                            .value(FieldValue.of((Integer) value)))));
                        } else if (value instanceof Map) {
                            Map<String, Object> nestedMap = (Map<String, Object>) value;
                            if (isRangeQuery(nestedMap)) {
                                BoolQuery.Builder rangeOrNullQuery = QueryBuilders.bool();
                                RangeQuery.Builder rangeQuery = QueryBuilders.range().field(field);
                                nestedMap.forEach((rangeOperator, rangeValue) -> {
                                    switch (rangeOperator) {
                                        case Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS:
                                            rangeQuery.gte(JsonData.of(rangeValue));
                                            break;
                                        case Constants.SEARCH_OPERATION_LESS_THAN_EQUALS:
                                            rangeQuery.lte(JsonData.of(rangeValue));
                                            break;
                                        case Constants.SEARCH_OPERATION_GREATER_THAN:
                                            rangeQuery.gt(JsonData.of(rangeValue));
                                            break;
                                        case Constants.SEARCH_OPERATION_LESS_THAN:
                                            rangeQuery.lt(JsonData.of(rangeValue));
                                            break;
                                    }
                                });
                                rangeOrNullQuery.should(rangeQuery.build()._toQuery());
                                rangeOrNullQuery.should(Query.of(q -> q.bool(b -> b.mustNot(Query.of(qn -> qn.exists(e -> e.field(field)))))));
                                boolQueryBuilder.must(rangeOrNullQuery.build()._toQuery());
                            } else {
                                nestedMap.forEach((nestedField, nestedValue) -> {
                                    String fullPath = field + "." + nestedField;
                                    String fullPathActual = NON_TEXT_FIELDS.contains(fullPath) ? fullPath + Constants.KEYWORD : fullPath;

                                    if (nestedValue instanceof Boolean) {
                                        boolQueryBuilder.must(Query.of(q -> q.term(t -> t.field(fullPathActual).value((Boolean) nestedValue))));
                                    } else if (nestedValue instanceof String) {
                                        List<FieldValue> termList = Collections.singletonList(FieldValue.of((String) nestedValue));
                                        boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(fullPathActual).terms((TermsQueryField) termList))));
                                    } else if (nestedValue instanceof ArrayList) {
                                        List<FieldValue> termList = ((ArrayList<String>) nestedValue).stream().map(FieldValue::of).collect(Collectors.toList());
                                        boolQueryBuilder.must(Query.of(q -> q.terms(t -> t.field(fullPathActual).terms(tq -> tq.value(termList)))));
                                    }
                                });
                            }
                        }
                    });
            mustNotQueries.forEach(mustNotQuery -> boolQueryBuilder.mustNot(mustNotQuery));
            boolQueries.forEach(boolQuery -> boolQueryBuilder.must(boolQuery));
        }
        return boolQueryBuilder;
    }

    private void addSortToSearchSourceBuilder(
            SearchCriteria searchCriteria, SearchRequest.Builder searchRequestBuilder) {
        if (isNotBlank(searchCriteria.getOrderBy()) && isNotBlank(searchCriteria.getOrderDirection())) {
            if (searchCriteria.getOrderBy().equalsIgnoreCase("search_count")) {
                SortOrder sortOrder =
                        Constants.ASC.equals(searchCriteria.getOrderDirection()) ? SortOrder.Asc : SortOrder.Desc;
                searchRequestBuilder.sort(SortOptions.of(so -> so
                        .field(f -> f
                                .field(searchCriteria.getOrderBy())
                                .order(sortOrder)
                        )
                ));
            } else {
                SortOrder sortOrder =
                        Constants.ASC.equals(searchCriteria.getOrderDirection()) ? SortOrder.Asc : SortOrder.Desc;
                searchRequestBuilder.sort(SortOptions.of(so -> so
                        .field(f -> f
                                .field(searchCriteria.getOrderBy() + Constants.KEYWORD)
                                .order(sortOrder)
                        )
                ));
            }
        }
    }

    private void addRequestedFieldsToSearchSourceBuilder(
            SearchCriteria searchCriteria, SearchRequest.Builder searchRequestBuilder) {
        if (searchCriteria.getRequestedFields() == null) {
            // Get all fields in response
            searchRequestBuilder.source(SourceConfig.of(sc -> sc.fetch(true)));
        } else {
            if (searchCriteria.getRequestedFields().isEmpty()) {
                log.error("Please specify at least one field to include in the results.");
            }
            searchRequestBuilder.source(SourceConfig.of(sc -> sc.filter(filter -> filter.includes(searchCriteria.getRequestedFields()))));
        }
    }

    private void addQueryStringToFilter(String searchString, BoolQuery.Builder boolQueryBuilder) {
        if (isNotBlank(searchString)) {
            Query wildcardQuery = Query.of(q -> q.wildcard(
                    WildcardQuery.of(w -> w
                            .field("searchTags.keyword")
                            .value("*" + searchString.toLowerCase() + "*"))
            ));
            boolQueryBuilder.must(wildcardQuery);
        }
    }

    private void addFacetsToSearchSourceBuilder(
            List<String> facets, SearchRequest.Builder searchRequestBuilder) {
        if (facets != null && !facets.isEmpty()) {
            Map<String, Aggregation> aggregationMap = facets.stream()
                    .collect(Collectors.toMap(
                            field -> field + "_agg",
                            field -> Aggregation.of(a -> a.terms(
                                    TermsAggregation.of(t -> t
                                            .field(NON_TEXT_FIELDS.contains(field) ? field + Constants.KEYWORD : field)
                                            .size(250)
                                    ))
                            )
                    ));
            searchRequestBuilder.aggregations(aggregationMap);
        }
    }

    private boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    @Override
    public void deleteDocumentsByCriteria(String esIndexName, Query query) {
        try {
            HitsMetadata<Object> searchHits = executeSearch(esIndexName, query);
            assert searchHits.total() != null;
            if (searchHits.total().value() > 0) {
                BulkResponse bulkResponse = deleteMatchingDocuments(esIndexName, searchHits);
                if (!bulkResponse.errors()) {
                    log.info("Documents matching the criteria deleted successfully from Elasticsearch.");
                } else {
                    log.error("Some documents failed to delete from Elasticsearch.");
                }
            } else {
                log.info("No documents match the criteria.");
            }
        } catch (Exception e) {
            log.error("Error occurred during deleting documents by criteria from Elasticsearch.", e);
        }
    }

    private HitsMetadata<Object> executeSearch(String esIndexName, Query query) throws IOException {
        SearchRequest searchRequest = new SearchRequest.Builder()
                .index(esIndexName)
                .query(query)
                .build();
        SearchResponse<Object> searchResponse =
                elasticsearchClient.search(searchRequest, Object.class);
        return searchResponse.hits();
    }

    private BulkResponse deleteMatchingDocuments(String esIndexName, HitsMetadata<Object> searchHits)
            throws IOException {
        List<BulkOperation> operations = new ArrayList<>();
        for (Hit<Object> hit : searchHits.hits()) {
            new DeleteRequest.Builder()
                    .index(esIndexName)
                    .id(hit.id())
                    .build();
            operations.add(new BulkOperation.Builder().delete(d -> d.index(esIndexName).id(hit.id())).build());
        }
        BulkRequest bulkRequest = new BulkRequest.Builder().operations(operations).build();
        return elasticsearchClient.bulk(bulkRequest);
    }

    private boolean isRangeQuery(Map<String, Object> nestedMap) {
        return nestedMap.keySet().stream().anyMatch(key -> key.equals(Constants.SEARCH_OPERATION_GREATER_THAN_EQUALS) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN_EQUALS) || key.equals(Constants.SEARCH_OPERATION_GREATER_THAN) ||
                key.equals(Constants.SEARCH_OPERATION_LESS_THAN));
    }

    private Query buildQueryPart(Map<String, Object> queryMap) {
        log.info("Search:: buildQueryPart");
        if (queryMap == null || queryMap.isEmpty()) {
            return QueryBuilders.matchAll().build()._toQuery();
        }
        for (Entry<String, Object> entry : queryMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            switch (key) {
                case Constants.BOOL:
                    return buildBoolQuery((Map<String, Object>) value)._toQuery();
                case Constants.TERM:
                    return buildTermQuery((Map<String, Object>) value);
                case Constants.TERMS:
                    return buildTermsQuery((Map<String, Object>) value);
                case Constants.MATCH:
                    return buildMatchQuery((Map<String, Object>) value);
                case Constants.RANGE:
                    return buildRangeQuery((Map<String, Object>) value);
                default:
                    throw new IllegalArgumentException(Constants.UNSUPPORTED_QUERY + key);
            }
        }

        return null;
    }

    private BoolQuery buildBoolQuery(Map<String, Object> boolMap) {
        log.info("Search:: builderBoolQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        if (boolMap.containsKey(Constants.MUST)) {
            List<Map<String, Object>> mustList = (List<Map<String, Object>>) boolMap.get("must");
            mustList.forEach(must -> boolQueryBuilder.must(buildQueryPart(must)));
        }
        if (boolMap.containsKey(Constants.FILTER)) {
            List<Map<String, Object>> filterList = (List<Map<String, Object>>) boolMap.get("filter");
            filterList.forEach(filter -> boolQueryBuilder.filter(buildQueryPart(filter)));
        }
        if (boolMap.containsKey(Constants.MUST_NOT)) {
            List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) boolMap.get("must_not");
            mustNotList.forEach(mustNot -> boolQueryBuilder.mustNot(buildQueryPart(mustNot)));
        }
        if (boolMap.containsKey(Constants.SHOULD)) {
            List<Map<String, Object>> shouldList = (List<Map<String, Object>>) boolMap.get("should");
            shouldList.forEach(should -> boolQueryBuilder.should(buildQueryPart(should)));
        }

        return boolQueryBuilder.build();
    }

    private Query buildTermQuery(Map<String, Object> termMap) {
        log.info("search::buildTermQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : termMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.term(t -> t.field(entry.getKey()).value((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildTermsQuery(Map<String, Object> termsMap) {
        log.info("search:: buildTermsQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : termsMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.terms(t -> t.field(entry.getKey()).terms((TermsQueryField) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildMatchQuery(Map<String, Object> matchMap) {
        log.info("search:: buildMatchQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : matchMap.entrySet()) {
            boolQueryBuilder.must(QueryBuilders.match(m -> m.field(entry.getKey()).query((FieldValue) entry.getValue())));
        }
        return boolQueryBuilder.build()._toQuery();
    }

    private Query buildRangeQuery(Map<String, Object> rangeMap) {
        log.info("search:: buildRangeQuery");
        BoolQuery.Builder boolQueryBuilder = QueryBuilders.bool();
        for (Entry<String, Object> entry : rangeMap.entrySet()) {
            Map<String, Object> rangeConditions = (Map<String, Object>) entry.getValue();
            RangeQuery.Builder rangeQueryBuilder = new RangeQuery.Builder().field(entry.getKey());
            rangeConditions.forEach((condition, value) -> {
                switch (condition) {
                    case "gt":
                        rangeQueryBuilder.gt(JsonData.of(value));
                        break;
                    case "gte":
                        rangeQueryBuilder.gte(JsonData.of(value));
                        break;
                    case "lt":
                        rangeQueryBuilder.lt(JsonData.of(value));
                        break;
                    case "lte":
                        rangeQueryBuilder.lte(JsonData.of(value));
                        break;
                    default:
                        throw new IllegalArgumentException(Constants.UNSUPPORTED_RANGE + condition);
                }
            });
            boolQueryBuilder.must(rangeQueryBuilder.build()._toQuery());
        }
        return boolQueryBuilder.build()._toQuery();
    }

    @Override
    public boolean isIndexPresent(String indexName) {
        try {
            GetIndexRequest request = new GetIndexRequest.Builder().index(indexName).build();
            GetIndexResponse response = elasticsearchClient.indices().get(request);
            return response != null;
        } catch (IOException e) {
            log.error("Error checking if index exists", e);
            return false;
        }
    }

    @Override
    public BulkResponse saveAll(String esIndexName, List<JsonNode> entities) throws IOException {
        try {
            log.info("EsUtilServiceImpl :: saveAll");
            List<BulkOperation> operations = new ArrayList<>();
            entities.forEach(entity -> {
                String formattedId = entity.get(Constants.ID).asText();
                Map<String, Object> entityMap = objectMapper.convertValue(entity, Map.class);
                BulkOperation operation = BulkOperation.of(b -> b
                        .index(i -> i
                                .index(esIndexName)
                                .id(formattedId)
                                .document(entityMap)
                        )
                );
                operations.add(operation);
            });

            BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(operations));
            return elasticsearchClient.bulk(bulkRequest);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new CustomException("error bulk uploading", e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Override
    public Object readDocument(String esIndexName, String documentId) throws IOException {
        GetRequest getRequest = GetRequest.of(g -> g.index(esIndexName).id(documentId));
        GetResponse<Object> response = elasticsearchClient.get(getRequest, Object.class);
        if (!response.found()) {
            return "Document not found in Elasticsearch.";
        }
        return response.source();
    }


}

