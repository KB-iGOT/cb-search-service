package com.igot.cb.kafka.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.exceptions.CustomException;
import com.igot.cb.search.entity.TrendingSearches;
import com.igot.cb.search.entity.UserSearchInputs;
import com.igot.cb.search.repository.TrendingSearchesRepository;
import com.igot.cb.search.repository.UserSearchInputsRepository;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class KafkaConsumer {
    private final ObjectMapper mapper;
    private final CassandraOperation cassandraOperation;
    private final CbServerProperties configuration;
    private final EsUtilService esUtilService;
    private final UserSearchInputsRepository userSearchInputsRepository;
    private final TrendingSearchesRepository trendingSearchesRepository;

    @Autowired
    public KafkaConsumer(
            ObjectMapper mapper,
            CassandraOperation cassandraOperation,
            CbServerProperties configuration,
            EsUtilService esUtilService,
            UserSearchInputsRepository userSearchInputsRepository,
            TrendingSearchesRepository trendingSearchesRepository
    ) {
        this.mapper = mapper;
        this.cassandraOperation = cassandraOperation;
        this.configuration = configuration;
        this.esUtilService = esUtilService;
        this.userSearchInputsRepository = userSearchInputsRepository;
        this.trendingSearchesRepository = trendingSearchesRepository;
    }

    @KafkaListener(groupId = "${kafka.topic.user.recent.searches.group}", topics = "${kafka.topic.user.recent.searches}")
    public void demandContentConsumer(ConsumerRecord<String, String> data) {
        try {
            Map<String, String> requestPayload = mapper.readValue(data.value(), new TypeReference<Map<String, String>>() {});
            CompletableFuture.runAsync(() -> {
                processRequest(requestPayload);
            });
        } catch (Exception e) {
            log.error("Failed to read demand request. Message received : " + data.value(), e);
        }
    }

    public void processRequest(Map<String, String> requestPayload) {
        log.info("Processing request for searchId: " + requestPayload.get(Constants.ID));
        String searchId = requestPayload.get(Constants.ID);
        try {
            Map<String, Object> readResponse = (Map<String, Object>) esUtilService.readDocument(Constants.TRENDING_SEARCHES_INDEX_NAME, searchId);
            if (readResponse == null) {
                log.error("No record found for searchId: " + searchId);
            } else {
                saveRecordsToPostgres(readResponse,requestPayload);
            }
        } catch (IOException e) {
            log.error("Error while updating in cassandra for searchId: {} {}", searchId, e.getMessage());
            throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    private void saveRecordsToPostgres(Map<String, Object> readResponse, Map<String, String> requestPayload) {
        try {
            Object countObj = readResponse.get(Constants.SEARCH_COUNT);
            Long count = ((Number) countObj).longValue();
            TrendingSearches trendingSearches = new TrendingSearches();
            trendingSearches.setQuery(readResponse.get(Constants.PROCESSED_QUERY).toString());
            trendingSearches.setLastSearched(readResponse.get(Constants.LAST_SEARCHED).toString());
            trendingSearches.setSearchCount(count);
            trendingSearchesRepository.save(trendingSearches);

            UserSearchInputs userSearchInputs = new UserSearchInputs();
            userSearchInputs.setRawQuery(requestPayload.get(Constants.ACTUAL_QUERY));
            userSearchInputs.setProcessedQuery(readResponse.get(Constants.PROCESSED_QUERY).toString());
            userSearchInputs.setUpdatedOn(readResponse.get(Constants.LAST_SEARCHED).toString());
            userSearchInputsRepository.save(userSearchInputs);

        }catch (Exception e){
            log.error("Error while saving record to postgres for searchId: {} {}", readResponse.get("id"), e.getMessage());
            throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
