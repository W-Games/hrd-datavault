package com.hardrock.games.model.input.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hardrock.games.model.constant.AppName;
import com.hardrock.games.model.constant.EventName;
import com.hardrock.games.model.input.InputPubSubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class TestEventMessageTest {



    @Test
    public void TestSuccessInstanceCreate() throws JsonProcessingException, NoSuchAlgorithmException {
        String inputJsonString = "{\"app_name\":\"NC\", \"event_name\":\"unity_login\", \"event_payload\":{\"uid\":\"some_uid\",\"email\":\"some@mail.com\"}}";
        PubsubMessage psMessage = new PubsubMessage(inputJsonString.getBytes(),null,null, null);
        TestEventMessage inputMessage = new TestEventMessage(psMessage);
        inputMessage.getAppName();

        assertEquals( inputMessage.getAppName() == AppName.NEVERLAND_CASINO, true);
        assertEquals( inputMessage.getEventName() == EventName.UNITY_LOGIN, true);
        assertEquals(Objects.equals(inputMessage.getUid(), "some_uid"), true);
        assertEquals(Objects.equals(inputMessage.getEmail(), "some@mail.com"), true);

    }
}