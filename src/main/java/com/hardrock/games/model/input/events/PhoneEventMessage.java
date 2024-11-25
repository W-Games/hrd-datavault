package com.hardrock.games.model.input.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hardrock.games.model.input.InputPubSubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.hardrock.games.helpers.HelperFunctions.HashCalc;

public class PhoneEventMessage extends InputPubSubMessage implements Serializable {

    private String uid;
    private String phone;

    private String hkey_uid;
    private String hkey_phone;
    private String hkey_link;

    public PhoneEventMessage(PubsubMessage psMessage ) throws JsonProcessingException, NoSuchAlgorithmException {
        super(psMessage);

        String jsonString = new String(psMessage.getPayload(), StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonString).get("event_payload");
        this.uid = root.get("uid").asText();
        this.phone = root.get("phone").asText();


        this.hkey_uid = HashCalc(this.uid);

        this.hkey_phone = HashCalc(this.phone);

        this.hkey_link = HashCalc(this.uid, this.phone);



    }

    public String getPhone() {
        return phone;
    }

    public String getUid() {
        return uid;
    }

    public String getHkey_phone() {
        return hkey_phone;
    }

    public String getHkey_uid() {
        return hkey_uid;
    }

    public String getHkey_link() {
        return hkey_link;
    }
}
