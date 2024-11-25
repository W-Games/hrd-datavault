package com.hardrock.games.model.input;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hardrock.games.model.constant.AppName;
import com.hardrock.games.model.constant.EventName;
import jdk.jfr.Event;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;

public class InputPubSubMessage implements Serializable {

    private String messageId;
    private AppName appName;
    private EventName eventName;



    public InputPubSubMessage(PubsubMessage psMessage ) throws JsonProcessingException {



        this.messageId = psMessage.getMessageId();
        String jsonString = new String(psMessage.getPayload(), StandardCharsets.UTF_8);
//
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonString);
        this.appName = AppName.fromString(root.get("app_name").asText());
        this.eventName = EventName.fromString(root.get("event_name").asText());
//        this.email = root.get("email").asText();
//
//        MessageDigest digest = MessageDigest.getInstance("SHA-256");
//
//        this.hkey_uid = Hashing.sha256()
//                .hashString(this.uid, StandardCharsets.UTF_8)
//                .toString();
//
//        this.hkey_email = Hashing.sha256()
//                .hashString(this.email, StandardCharsets.UTF_8)
//                .toString();
//
//        this.hkey_link = Hashing.sha256()
//                .hashString(this.hkey_uid+this.hkey_email, StandardCharsets.UTF_8)
//                .toString();



    }

    public String getMessageId() {
        return this.messageId;
    }
    public AppName getAppName() {return this.appName;}
    public EventName getEventName() {return this.eventName;}


}
