package com.igot.cb.search.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.ApiResponse;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.redis.cache.CacheService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.util.*;

@ExtendWith(MockitoExtension.class)
class SearchServiceImplTest {
    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private CacheService cacheService;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private SearchServiceImpl searchServiceImpl;

    @Mock
    private CbServerProperties cbServerProperties;

    @Test
    void createUserRecentSearches_savesSearchSuccessfully() throws JsonProcessingException {
        String token = "validToken";
        String userId = "user123";
        JsonNode searchQuery = new ObjectMapper().readTree("{\"nlpSearchQuery\":\"query1\",\"searchCategory\":\"category1\",\"searchQuery\":\"query1\"}");

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(cassandraOperation.getRecordsByOrder(
                eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.TABLE_USER_RECENT_SEARCH),
                anyMap(),
                isNull(),
                isNull()
        )).thenReturn(Collections.emptyList());
        ApiResponse mockApiResponse = new ApiResponse();
        mockApiResponse.put(Constants.RESPONSE, Constants.SUCCESS);

        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(mockApiResponse);

        ApiResponse response = searchServiceImpl.createUserRecentSearches(searchQuery, token);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
    }

    @Test
    void createUserRecentSearches_returnsErrorWhenTokenIsInvalid() {
        String token = "invalidToken";
        JsonNode searchQuery = new ObjectMapper().createObjectNode();

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(Constants.UNAUTHORIZED);

        ApiResponse response = searchServiceImpl.createUserRecentSearches(searchQuery, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrMsg());
    }

    @Test
    void readUserRecentSearches_returnsCachedSearches() throws JsonProcessingException {
        String token = "validToken";
        String userId = "user123";
        String cachedJson = "[{\"searchQuery\":\"query1\"}]";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(cacheService.getCache(userId)).thenReturn(cachedJson);
        List<Map<String, Object>> expectedList = List.of(Map.of("searchQuery", "query1"));

        when(cacheService.getCache(userId)).thenReturn(cachedJson);
        doReturn(expectedList).when(objectMapper).readValue(eq(cachedJson), any(TypeReference.class));

        ApiResponse response = searchServiceImpl.readUserRecentSearches(token);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertNotNull(response.getResult().get("searchQueries"));
    }

    @Test
    void deleteUserAllRecentSearches_deletesSuccessfully() {
        String token = "validToken";
        String userId = "user123";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        // Sample active record
        Map<String, Object> searchRecord = new HashMap<>();
        searchRecord.put("user_id", userId);
        searchRecord.put("timestamp", 123456789L);
        searchRecord.put("search_query", "test");
        searchRecord.put("is_active", true);
        List<Map<String, Object>> records = List.of(searchRecord);

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                anyString(), anyString(), anyMap(), isNull(), isNull())).thenReturn(records);

        when(cassandraOperation.deleteRecord(anyString(), anyString(), anyMap()))
                .thenReturn(Collections.singletonMap(Constants.RESPONSE, Constants.SUCCESS));

        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(Collections.singletonMap(Constants.RESPONSE, Constants.SUCCESS));

        ApiResponse response = searchServiceImpl.deleteUserAllRecentSearches(token);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("All recent searches deleted successfully", response.getResult().get("result"));
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());

        verify(cacheService).deleteCache(userId);
    }


    @Test
    void deleteUserRecentSearchesByTimestamp_deletesSuccessfully() {
        String token = "validToken";
        String userId = "user123";
        Long timestamp = 123456789L;

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        // Prepare record
        Map<String, Object> searchRecord = new HashMap<>();
        searchRecord.put("user_id", userId);
        searchRecord.put("timestamp", timestamp);
        searchRecord.put("search_query", "AI");
        searchRecord.put("nlp_search_query", "Artificial Intelligence");
        searchRecord.put("search_category", Set.of("course"));
        searchRecord.put("is_active", true);

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                anyString(), anyString(), anyMap(), isNull(), isNull())
        ).thenReturn(List.of(searchRecord));

        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(Collections.singletonMap(Constants.RESPONSE, Constants.SUCCESS));

        when(cassandraOperation.deleteRecord(anyString(), anyString(), anyMap()))
                .thenReturn(Collections.singletonMap(Constants.RESPONSE, Constants.SUCCESS));

        ApiResponse response = searchServiceImpl.deleteUserRecentSearchesByTimestamp(token, timestamp);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("Recent search deleted successfully", response.getResult().get("result"));
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());

        verify(cacheService).deleteCache(userId);
    }


    @Test
    void createUserRecentSearches_returnsErrorWhenSearchQueryIsNull() {
        String token = "validToken";
        String userId = "user123";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        ApiResponse response = searchServiceImpl.createUserRecentSearches(null, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("search query is empty", response.getParams().getErrMsg());
    }

    @Test
    void createUserRecentSearches_returnsErrorWhenRequiredFieldsAreMissing() throws JsonProcessingException {
        String token = "validToken";
        String userId = "user123";
        JsonNode searchQuery = new ObjectMapper().readTree("{\"nlpSearchQuery\":\"\",\"searchCategory\":\"\",\"searchQuery\":\"\"}");

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        ApiResponse response = searchServiceImpl.createUserRecentSearches(searchQuery, token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("One or more required fields (nlpSearchQuery, searchCategory, searchQuery) are missing or empty", response.getParams().getErrMsg());
    }

    @Test
    void readUserRecentSearches_returnsErrorWhenTokenIsInvalid() {
        String token = "invalidToken";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(Constants.UNAUTHORIZED);

        ApiResponse response = searchServiceImpl.readUserRecentSearches(token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrMsg());
    }

    @Test
    void readUserRecentSearches_returnsEmptyWhenNoSearchesExist() {
        String token = "validToken";
        String userId = "user123";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(cbServerProperties.getRecentSearchesLimit()).thenReturn(10);
        when(cassandraOperation.getRecordsByOrder(anyString(), anyString(), anyMap(), anyInt(), any()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = searchServiceImpl.readUserRecentSearches(token);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals("User dont have any recent searches", response.getParams().getErrMsg());
    }

    @Test
    void deleteUserAllRecentSearches_returnsErrorWhenTokenIsInvalid() {
        String token = "invalidToken";

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(Constants.UNAUTHORIZED);

        ApiResponse response = searchServiceImpl.deleteUserAllRecentSearches(token);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrMsg());
    }

    @Test
    void deleteUserRecentSearchesByTimestamp_returnsErrorWhenTokenIsInvalid() {
        String token = "invalidToken";
        Long timestamp = 123456789L;

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(Constants.UNAUTHORIZED);

        ApiResponse response = searchServiceImpl.deleteUserRecentSearchesByTimestamp(token, timestamp);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrMsg());
    }

    @Test
    void deleteUserRecentSearchesByTimestamp_returnsErrorWhenDeleteFails() {
        String token = "validToken";
        String userId = "user123";
        Long timestamp = 123456789L;

        // Mock verified user
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        // Mock that a record exists
        Map<String, Object> existingRecord = new HashMap<>();
        existingRecord.put(Constants.USERID, userId);
        existingRecord.put(Constants.TIMESTAMP, timestamp);
        existingRecord.put(Constants.SEARCH_QUERY, "AI course");
        existingRecord.put(Constants.NLP_SEARCH_QUERY, "artificial intelligence");
        existingRecord.put(Constants.IS_ACTIVE, true);
        existingRecord.put(Constants.SEARCH_CATEGORY, Set.of("course"));

        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(List.of(existingRecord));

        // Mock insert succeeds
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.SUCCESS));

        // Mock delete fails
        when(cassandraOperation.deleteRecord(anyString(), anyString(), anyMap()))
                .thenReturn(Map.of(Constants.RESPONSE, Constants.FAILED, "errmsg", "Delete operation failed"));

        ApiResponse response = searchServiceImpl.deleteUserRecentSearchesByTimestamp(token, timestamp);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("Delete operation failed", response.getParams().getErrMsg());
    }

    @Test
    void testDuplicateRecordInactive() {
        String userId = "user123";
        String token = "validToken";
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        Map<String, Object> record = new HashMap<>();
        record.put(Constants.SEARCH_QUERY, "java");
        record.put(Constants.IS_ACTIVE, false);
        record.put(Constants.TIMESTAMP, 12345L);

        when(cassandraOperation.getRecordsByOrder(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));

        ApiResponse mockApiResponse = new ApiResponse();
        mockApiResponse.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(mockApiResponse);

        searchServiceImpl.createUserRecentSearches(buildSearchQueryJson(), token);

        verify(cassandraOperation).deleteRecord(eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.TABLE_USER_RECENT_SEARCH),
                argThat(map -> map.get(Constants.IS_ACTIVE).equals(false)));
    }

    @Test
    void testDuplicateRecordActiveWithNullCategory() {
        String userId = "user123";
        String token = "validToken";
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        Map<String, Object> record = new HashMap<>();
        record.put(Constants.SEARCH_QUERY, "java");
        record.put(Constants.IS_ACTIVE, true);
        record.put(Constants.TIMESTAMP, 12345L);
        record.put(Constants.SEARCH_CATEGORY, null);

        when(cassandraOperation.getRecordsByOrder(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));

        ApiResponse mockApiResponse = new ApiResponse();
        mockApiResponse.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(mockApiResponse);

        searchServiceImpl.createUserRecentSearches(buildSearchQueryJson(), token);

        verify(cassandraOperation).deleteRecord(any(), any(), any());
    }

    @Test
    void testDuplicateRecordActiveWithCategories() {
        String userId = "user123";
        String token = "validToken";
        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);

        Set<String> categories = new HashSet<>(Arrays.asList("books", "java"));
        Map<String, Object> record = new HashMap<>();
        record.put(Constants.SEARCH_QUERY, "java");
        record.put(Constants.IS_ACTIVE, true);
        record.put(Constants.TIMESTAMP, 12345L);
        record.put(Constants.SEARCH_CATEGORY, categories);

        when(cassandraOperation.getRecordsByOrder(any(), any(), any(), any(), any()))
                .thenReturn(List.of(record));

        ApiResponse mockApiResponse = new ApiResponse();
        mockApiResponse.put(Constants.RESPONSE, Constants.SUCCESS);
        when(cassandraOperation.insertRecord(anyString(), anyString(), anyMap()))
                .thenReturn(mockApiResponse);

        searchServiceImpl.createUserRecentSearches(buildSearchQueryJson(), token);

        verify(cassandraOperation).deleteRecord(any(), any(), any());
        // optional: verify merged categories
        verify(cassandraOperation).insertRecord(
                anyString(),
                anyString(),
                argThat(map -> ((Set<String>) map.get(Constants.SEARCH_CATEGORY)).containsAll(categories))
        );
    }

    @Test
    void deleteUserRecentSearchesByTimestamp_returnsNotFoundWhenNoRecords() {
        String token = "validToken";
        String userId = "user123";
        Long timestamp = 987654321L;

        when(accessTokenValidator.verifyUserToken(token)).thenReturn(userId);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                anyString(), anyString(), anyMap(), isNull(), isNull()))
                .thenReturn(Collections.emptyList());

        ApiResponse response = searchServiceImpl.deleteUserRecentSearchesByTimestamp(token, timestamp);

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals("No recent search found for the given userid and timestamp", response.getParams().getErrMsg());
    }


    private JsonNode buildSearchQueryJson() {
        try {
            String json = "{"
                    + "\"nlpSearchQuery\":\"java\","
                    + "\"searchCategory\":\"category1\","
                    + "\"searchQuery\":\"java\""
                    + "}";
            return new ObjectMapper().readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
