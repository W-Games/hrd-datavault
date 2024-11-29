package com.hardrock.games.helpers;

import com.hardrock.games.model.constant.AppName;
import com.hardrock.games.model.constant.EventName;
import org.junit.Test;

import java.util.Map;


import static org.junit.Assert.assertEquals;

public class HelperFunctionsTest {

    @Test
    public void ParseEventNameFromAttributes() {

        assertEquals( HelperFunctions.GetEventNameFromAttributes(Map.of("event_name", EventName.SPIN.getText())) == EventName.SPIN, true);
        assertEquals( HelperFunctions.GetEventNameFromAttributes(null) == EventName.UNDEFINED, true);
        assertEquals( HelperFunctions.GetEventNameFromAttributes(Map.of("event_name", "not_existing_event_name")) == EventName.UNDEFINED, true);
        assertEquals( HelperFunctions.GetEventNameFromAttributes(Map.of("eve_name", EventName.SPIN.getText())) == EventName.UNDEFINED, true);

    }
}
