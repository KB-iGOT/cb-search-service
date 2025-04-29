package com.igot.cb.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.elasticsearch.service.EsUtilService;
import com.igot.cb.exceptions.CustomException;
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
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class KafkaConsumer {
    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private CbServerProperties configuration;

    @Autowired
    private EsUtilService esUtilService;

    @KafkaListener(groupId = "${kafka.topic.user.recent.searches.group}", topics = "${kafka.topic.user.recent.searches}")
    public void demandContentConsumer(ConsumerRecord<String, String> data) {
        try {
            String searchRequest = mapper.readValue(data.value(), String.class);
            CompletableFuture.runAsync(() -> {
                processRequest(searchRequest);
            });
        } catch (Exception e) {
            log.error("Failed to read demand request. Message received : " + data.value(), e);
        }
    }

    public void processRequest(String searchId) {
        try {
            Object readResponse = esUtilService.readDocument(Constants.TRENDING_SEARCHES_INDEX_NAME, searchId);
            if (readResponse == null) {
                log.error("No record found for searchId: " + searchId);
            } else {
                Map<String, Object> searchQuery = (Map<String, Object>) readResponse;
                Object lastSearchedObj = searchQuery.get(Constants.LAST_SEARCHED);
                if (lastSearchedObj instanceof String lastSearchedStr) {
                    try {
                        Instant instant = Instant.parse(lastSearchedStr);  // Parse the ISO8601 string
                        searchQuery.put(Constants.LAST_SEARCHED, instant);           // Replace the value in the map
                    } catch (DateTimeParseException e) {
                        log.error("Failed to parse last_searched timestamp for searchId {}: {}", searchId, e.getMessage());
                        searchQuery.remove(Constants.LAST_SEARCHED);  // Or handle differently
                    }
                }
                Object response = cassandraOperation.insertRecord(
                        Constants.KEYSPACE_SUNBIRD_COURSES,
                        Constants.TABLE_TRENDING_SEARCH,
                        searchQuery);
                if (response == null) {
                    log.error("Failed to insert record into Cassandra for searchId: {}", searchId);
                } else {
                    log.info("Inserted record into Cassandra for searchId: {}", searchId);
                }
            }
        } catch (IOException e) {
            log.error("Error while updating in cassandra for searchId: {} {}", searchId, e.getMessage());
            throw new CustomException(Constants.ERROR, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }
}
