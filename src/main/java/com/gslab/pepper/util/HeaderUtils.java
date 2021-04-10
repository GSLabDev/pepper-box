package com.gslab.pepper.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.log.Logger;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class HeaderUtils {

    private static String MESSAGE_KEY = "MESSAGE_KEY";
    private static String STRING_DATA_TYPE = "java.lang.String";
    private static String SPRING_JSON_HEADER_TYPES = "spring_json_header_types";

    private static final Logger log = LoggingManager.getLoggerForClass();


    public Iterable<Header> generateHeaders(String messageHeaders) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Header> headers = new ArrayList<>();
        Map<String, String> headerTypes = new HashMap<>();
        if (!StringUtils.isEmpty(messageHeaders)) {
            JSONObject jsonHeaders = new JSONObject(messageHeaders);
            Iterator<String> keys = jsonHeaders.keys();
            while (keys.hasNext()) {
                String key = keys.next();
                if (!MESSAGE_KEY.equals(key)) {
                    headers.add(new RecordHeader(key, jsonHeaders.getString(key).getBytes(StandardCharsets.UTF_8)));
                    headerTypes.put(key, STRING_DATA_TYPE);
                }
            }
            headers.add(new RecordHeader(SPRING_JSON_HEADER_TYPES, objectMapper.writeValueAsString(headerTypes).getBytes(StandardCharsets.UTF_8)));
        }
        return headers;
    }

    public String getMessageKey(String messageHeaders) {
        if (!StringUtils.isEmpty(messageHeaders)) {
            return new JSONObject(messageHeaders).optString(MESSAGE_KEY, null);
        }
        return null;
    }
}
