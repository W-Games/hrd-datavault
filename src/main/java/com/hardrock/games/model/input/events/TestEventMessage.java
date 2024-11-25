package com.hardrock.games.model.input.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hardrock.games.model.input.InputPubSubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.hardrock.games.helpers.HelperFunctions.HashCalc;

public class TestEventMessage extends InputPubSubMessage implements Serializable {

    private String uid;
    private String email;

    private String hkey_uid;
    private String hkey_email;
    private String hkey_link;

    public TestEventMessage(PubsubMessage psMessage ) throws JsonProcessingException, NoSuchAlgorithmException {
        super(psMessage);

        String jsonString = new String(psMessage.getPayload(), StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonString).get("event_payload");
        this.uid = root.get("uid").asText();
        this.email = root.get("email").asText();

        MessageDigest digest = MessageDigest.getInstance("SHA-256");

        this.hkey_uid = HashCalc(this.uid);

        this.hkey_email = HashCalc(this.email);

        this.hkey_link = HashCalc(this.uid, this.email);



    }

    public String getEmail() {
        return email;
    }

    public String getUid() {
        return uid;
    }

    public String getHkey_email() {
        return hkey_email;
    }

    public String getHkey_uid() {
        return hkey_uid;
    }

    public String getHkey_link() {
        return hkey_link;
    }
}
