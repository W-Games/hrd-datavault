package com.hardrock.games.model.input.transformations;

import com.hardrock.games.helpers.HelperFunctions;
import com.hardrock.games.model.constant.EventName;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class PubsubTransformers {

    private static final Logger LOG = LoggerFactory.getLogger(PubsubTransformers.class);
//    public static DoFn<PubsubMessage, KV<EventName, PubsubMessage>> PubsubExtractEventName() {
//
//        public void processElement(PubsubMessage msg,
//                OutputReceiver<KV<EventName, PubsubMessage>> output) throws Exception {
//
//            UserProfileRealtime outp = new UserProfileRealtime(element.getUid(), element, null,
//                    null);
//            KV<EventName, PubsubMessage> kv = KV.of(element.getUid(), outp);
//            LOG.info("[PUBSUB INPUT][PURCHASE][userId=%s][messageId=%s][Message=%s]".formatted(
//                    outp.getUserId(),
//                    outp.getMessageId(),
//                    element.toJson()));
//            receiver.output(kv);
//        }
//    }

    public static MapElements<PubsubMessage, KV<EventName, PubsubMessage>> PubsubExtractEventName() {
        return MapElements.via(new SimpleFunction<PubsubMessage, KV<EventName, PubsubMessage>>() {
            @Override
            public KV<EventName, PubsubMessage> apply(PubsubMessage msg) {
                EventName eventTime = HelperFunctions.GetEventNameFromAttributes(msg.getAttributeMap());
                KV<EventName, PubsubMessage> outp = KV.of(eventTime, msg);
                LOG.info("Parsing input message name");
                return outp;
            }
        });
    }
}
