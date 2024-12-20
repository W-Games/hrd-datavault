package com.hardrock.games.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hardrock.games.model.constant.AppName;
import com.hardrock.games.model.constant.Constants;
import com.hardrock.games.model.constant.EventName;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HelperFunctions {


    public static String HashCalc(String... s) {
        return  Hashing.sha256()
                .hashString(String.join("", s), StandardCharsets.UTF_8)
                .toString();
    }

    public static EventName GetEventName(PubsubMessage ps) throws JsonProcessingException {
        String jsonString = new String(ps.getPayload(), StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        String eventName = mapper.readTree(jsonString).get("event_name").asText();
        return EventName.fromString(eventName);
    }

    public static EventName GetEventNameFromAttributes(Map<String,String> inpParameters) {
        EventName res;
        if (inpParameters != null) {
            String eventName = inpParameters.getOrDefault(Constants.ATTRIBUTES_EVENT_NAME,Constants.__DEFAULT_NOT_SET_VALUE);
            res =  EventName.fromString(eventName);
        } else {
            res = EventName.UNDEFINED;
        }
        return res;
    }

}
