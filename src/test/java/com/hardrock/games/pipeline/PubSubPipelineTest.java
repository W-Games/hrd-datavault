package com.hardrock.games.pipeline;
import com.hardrock.games.model.constant.EventName;
import com.hardrock.games.model.input.transformations.PubsubTransformers;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hardrock.games.pipeline.PipelineTest.WORDS_ARRAY;
import static java.util.Map.entry;


@RunWith(JUnit4.class)
public class PubSubPipelineTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    static PubsubMessage message1 = new PubsubMessage(
            "{\"app_name\":\"NC\", \"event_name\":\"unity_login\"}".getBytes(),Map.ofEntries(
            entry("event_name", EventName.UNITY_LOGIN.getText())
    ),"message_1", null);


    static final PubsubMessage[] INPUT_MESSAGES = new PubsubMessage[] {
            message1
    };

    static final List<PubsubMessage> MESSAGES = Arrays.asList(INPUT_MESSAGES);

    @Test
    public void myTest() {
        // Create a test pipeline.

        // Create an input PCollection.
        PCollection<PubsubMessage> input = pipeline.apply(Create.of(MESSAGES));

        // Apply the Count transform under test.
        PCollection<KV<EventName, PubsubMessage>> output =
                input.apply(PubsubTransformers.PubsubExtractEventName());
//                input.apply(Count.<EventName>perElement());

        PAssert.that(output).containsInAnyOrder(
                KV.of(EventName.UNITY_LOGIN,message1)
        );

        // Assert on the results.
//        PAssert.that(output)
//                .containsInAnyOrder(
//                        KV.of("hi", 5L),
//                        KV.of("there", 1L),
//                        KV.of("sue", 2L),
//                        KV.of("bob", 2L),
//                        KV.of("", 3L),
//                        KV.of("ZOW", 1L));

        // Run the pipeline.
        pipeline.run();

    }

}
