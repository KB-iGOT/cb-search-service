package com.igot.cb.search.service;

import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.elasticsearch.dto.SearchCriteria;
import com.igot.cb.elasticsearch.dto.SearchResult;
import com.igot.cb.kafka.producer.KafkaProducer;
import com.igot.cb.util.ApiResponse;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.ProjectUtil;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.units.qual.C;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@Service
public class SearchServiceImpl implements SearchService {

    @Autowired
    private AccessTokenValidator accessTokenValidator;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private EsUtilService esUtilService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private KafkaProducer kafkaProducer;

    @Override
    public ApiResponse createUserRecentSearches(JsonNode searchQuery, String token) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_RECENT_SEARCH_CREATE);
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }

        if (searchQuery == null || searchQuery.isEmpty()) {
            response.getParams().setErrMsg("search query is empty");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.USERID, userId);

        List<Map<String, Object>> existingSearches = cassandraOperation.getRecordsByOrder(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_RECENT_SEARCH,
                propertyMap,
                10,
                null
        );
        String newSearchQuery = searchQuery.get(Constants.SEARCH_QUERY).asText();
        boolean isDuplicate = existingSearches.stream()
                .anyMatch(record -> newSearchQuery.equalsIgnoreCase((String) record.get("search_query")));
        JsonNode categoryNode = searchQuery.get(Constants.SEARCH_CATEGORY);
        JsonNode actualQuery = searchQuery.get(Constants.SEARCH_CATEGORY);
        Set<String> categorySet = new HashSet<>();
        Set<String> actualQuerySet = new HashSet<>();
        if (!isDuplicate) {
            if (isValidCategory(categoryNode)) {
                categorySet.add(categoryNode.asText());
            } else {
                response.getParams().setErrMsg("search category is empty");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> userSearchQuery = new HashMap<>();
            userSearchQuery.put(Constants.USERID, userId);
            userSearchQuery.put(Constants.UNIQUE_ID, System.currentTimeMillis()); // Generate new unique_id
            userSearchQuery.put(Constants.SEARCH_QUERY_KEY, searchQuery.get(Constants.SEARCH_QUERY).asText());
            userSearchQuery.put(Constants.SEARCH_CATEGORY, categorySet);

            ApiResponse insertResponse = (ApiResponse) cassandraOperation.insertRecord(
                    Constants.KEYSPACE_SUNBIRD_COURSES,
                    Constants.TABLE_USER_RECENT_SEARCH,
                    userSearchQuery
            );
            if (Constants.SUCCESS.equals(insertResponse.get(Constants.RESPONSE))) {
//                String id = userId + "_" + userSearchQuery.get(Constants.UNIQUE_ID);
//                esUtilService.addDocument(Constants.RECENT_SEARCHES_INDEX_NAME, Constants.INDEX_TYPE, id, userSearchQuery, cbServerProperties.getElasticSearchRecentJsonPath());
                response.getParams().setErrMsg("search request saved successfully");
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
                return response;
            } else {
                response.getParams().setErrMsg("Failed to save search query.");
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

        } else {
            if (isValidCategory(categoryNode)) {
                categorySet.add(categoryNode.asText());
            }
            Map<String, Object> existingRecord = existingSearches.stream()
                    .filter(record -> newSearchQuery.equalsIgnoreCase((String) record.get("search_query")))
                    .findFirst().orElse(null);
            if (existingRecord != null) {
                Map<String, Object> compositeMap = new HashMap<>();
                compositeMap.put(Constants.USERID, userId);
                compositeMap.put(Constants.UNIQUE_ID, existingRecord.get(Constants.UNIQUE_ID));
                cassandraOperation.deleteRecord(Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_RECENT_SEARCH,
                        compositeMap
                );
                esUtilService.deleteDocument(Constants.RECENT_SEARCHES_INDEX_NAME,
                        userId + "_" + existingRecord.get(Constants.UNIQUE_ID));
                Map<String, Object> updateFields = new HashMap<>();
                categorySet.addAll((Set<String>) existingRecord.get(Constants.SEARCH_CATEGORY));
                updateFields.put(Constants.USERID, userId);
                updateFields.put(Constants.UNIQUE_ID, System.currentTimeMillis()); // Generate new unique_id
                updateFields.put(Constants.SEARCH_QUERY_KEY, searchQuery.get(Constants.SEARCH_QUERY).asText());
                updateFields.put(Constants.SEARCH_CATEGORY, categorySet);
                ApiResponse updateResponse = (ApiResponse) cassandraOperation.insertRecord(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_USER_RECENT_SEARCH,
                        updateFields
                );
                if (Constants.SUCCESS.equals(updateResponse.get(Constants.RESPONSE))) {
                    String id = userId + "_" + updateFields.get(Constants.UNIQUE_ID);
                    esUtilService.addDocument(Constants.RECENT_SEARCHES_INDEX_NAME, Constants.INDEX_TYPE, id, updateFields, cbServerProperties.getElasticSearchRecentJsonPath());
                    response.getParams().setErrMsg("search request updated successfully");
                    response.getParams().setStatus(Constants.SUCCESS);
                    response.setResponseCode(HttpStatus.OK);
                    return response;
                } else {
                    response.getParams().setErrMsg("Failed to update search query.");
                    response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                    return response;
                }

            }
            response.getParams().setErrMsg("search request saved successfully");
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
            return response;
        }
    }

    private boolean isValidCategory(JsonNode node) {
        return node != null && !node.isNull() && !node.asText().trim().isEmpty();
    }

    @Override
    public ApiResponse readUserRecentSearches(String token) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_RECENT_SEARCH_READ);
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.USERID, userId);
        List<Map<String, Object>> userSearchList = cassandraOperation.getRecordsByOrder(Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_RECENT_SEARCH,
                propertyMap,
                10,
                Constants.ORDER_DESC
        );

        if (!userSearchList.isEmpty()) {
            response.put("searchQueries", userSearchList);
        } else {
            response.getParams().setErrMsg("User dont have any recent searches");
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
            return response;
        }
        response.setResponseCode(HttpStatus.OK);
        response.setResult(response.getResult());
        return response;
    }

    @Override
    public ApiResponse deleteUserRecentSearches(String token) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_RECENT_SEARCH_DELETE);
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.USERID, userId);
        Map<String, Object> deleteResponse = cassandraOperation.deleteRecord(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_RECENT_SEARCH,
                propertyMap
        );
        Query query = MatchQuery.of(m -> m.field("user_id"+Constants.KEYWORD).query(userId))._toQuery();
        esUtilService.deleteDocumentsByCriteria(Constants.RECENT_SEARCHES_INDEX_NAME,query);
        response.setResponseCode(HttpStatus.OK);
        response.setResult(deleteResponse);
        return response;
    }


    @Override
    public ApiResponse createUserTrendingSearches(JsonNode searchQuery) {
        JsonNode queryNode = searchQuery.get(Constants.PROCESSED_QUERY);
        String query = queryNode.asText();
        String id = query.trim().toLowerCase().replaceAll("\\s+", " ");
        String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8);
        Map<String, Object> trendingSearch = new HashMap<>();
        trendingSearch.put(Constants.PROCESSED_QUERY, searchQuery.get(Constants.PROCESSED_QUERY).asText());
        ApiResponse response = esUtilService.upsertTrendingSearch(Constants.TRENDING_SEARCHES_INDEX_NAME, encodedId, trendingSearch, cbServerProperties.getElasticSearchTrendingJsonPath());
        if (response.getParams().getStatus().equalsIgnoreCase(Constants.SUCCESS)) {
            Map<String, String> kafkaPayload = new HashMap<>();
            kafkaPayload.put(Constants.ID, id);
            kafkaPayload.put(Constants.ACTUAL_QUERY,searchQuery.get(Constants.ACTUAL_QUERY).asText() );
            kafkaProducer.push(cbServerProperties.getUserRecentSearchTopic(), kafkaPayload);
        }
        return response;
    }

    @Override
    public ApiResponse readUserTrendingSearches(SearchCriteria searchCriteria) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_TRENDING_SEARCH_READ);
        try {
            if(!esUtilService.isIndexPresent(Constants.TRENDING_SEARCHES_INDEX_NAME)){
                response.getParams().setErrMsg("Index not present,Please add the record to create the index");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }
            SearchResult searchResult =
                    esUtilService.searchDocuments(Constants.TRENDING_SEARCHES_INDEX_NAME, searchCriteria);
            Map<String, Object> jsonMap =
                    objectMapper.convertValue(searchResult, new TypeReference<Map<String, Object>>() {
                    });
            response.setResult(jsonMap);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            response.getParams().setErrMsg(e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public ApiResponse deleteUserRecentSearchesByUniqueId(String token, String uniqueId) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_RECENT_SEARCH_DELETE);
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId) || userId.equalsIgnoreCase(Constants.UNAUTHORIZED)) {
            response.getParams().setErrMsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.USERID, userId);
        propertyMap.put(Constants.UNIQUE_ID, uniqueId);
        Map<String, Object> deleteResponse = cassandraOperation.deleteRecord(
                Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_USER_RECENT_SEARCH,
                propertyMap
        );
        String id = userId + "_" + uniqueId;
        esUtilService.deleteDocument(Constants.RECENT_SEARCHES_INDEX_NAME,id);
        response.setResponseCode(HttpStatus.OK);
        response.setResult(deleteResponse);
        return response;
    }

    @Override
    public ApiResponse searchUserRecentSearches(SearchCriteria searchCriteria) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_TRENDING_SEARCH_READ);
        try {
            if(!esUtilService.isIndexPresent(Constants.RECENT_SEARCHES_INDEX_NAME)){
                response.getParams().setErrMsg("Index not present,Please add the record to create the index");
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }
            SearchResult searchResult =
                    esUtilService.searchDocuments(Constants.RECENT_SEARCHES_INDEX_NAME, searchCriteria);
            Map<String, Object> jsonMap =
                    objectMapper.convertValue(searchResult, new TypeReference<Map<String, Object>>() {
                    });
            response.setResult(jsonMap);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            response.getParams().setErrMsg(e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

}
