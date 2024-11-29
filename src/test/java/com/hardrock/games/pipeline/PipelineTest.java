package com.hardrock.games.pipeline;
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
import java.util.List;


@RunWith(JUnit4.class)
public class PipelineTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();



    static final String[] WORDS_ARRAY = new String[] {
            "hi", "there", "hi", "hi", "sue", "bob",
            "hi", "sue", "", "", "ZOW", "bob", ""};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    @Test
    public void myTest() {
        // Create a test pipeline.

        // Create an input PCollection.
        PCollection<String> input = pipeline.apply(Create.of(WORDS));

        // Apply the Count transform under test.
        PCollection<KV<String, Long>> output =
                input.apply(Count.<String>perElement());

        // Assert on the results.
        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("hi", 5L),
                        KV.of("there", 1L),
                        KV.of("sue", 2L),
                        KV.of("bob", 2L),
                        KV.of("", 3L),
                        KV.of("ZOW", 1L));

        // Run the pipeline.
        pipeline.run();

    }

}
