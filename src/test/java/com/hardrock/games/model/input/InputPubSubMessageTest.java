package com.hardrock.games.model.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hardrock.games.AppTest;
import com.hardrock.games.model.constant.AppName;
import com.hardrock.games.model.constant.EventName;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InputPubSubMessageTest
{

//    @Test
//    public void testTransactions() throws Throwable {
//
//        String initialList = UserState.updateTransactionList("","tid1");
//        assertEquals(initialList, "tid1");
//
//        String addTransactionOne = UserState.updateTransactionList(initialList,"tid2");
//        assertEquals("tid1;tid2",addTransactionOne );
//
//        String addTransactionTwo = UserState.updateTransactionList(addTransactionOne,"tid3");
//        assertEquals("tid1;tid2;tid3",addTransactionTwo);
//
//        String addTransactionThree = UserState.updateTransactionList(addTransactionTwo,"tid4");
//        assertEquals("tid2;tid3;tid4",addTransactionThree);
//
//
//
//    }



    @Test
    public void TestSuccessInstanceCreate() throws JsonProcessingException {
        String inputJsonString = "{\"app_name\":\"NC\", \"event_name\":\"unity_login\"}";
        PubsubMessage psMessage = new PubsubMessage(inputJsonString.getBytes(),null,null, null);
        InputPubSubMessage inputMessage = new InputPubSubMessage(psMessage);
        inputMessage.getAppName();

        assertEquals( inputMessage.getAppName() == AppName.NEVERLAND_CASINO, true);
        assertEquals( inputMessage.getEventName() == EventName.UNITY_LOGIN, true);
    }
}
