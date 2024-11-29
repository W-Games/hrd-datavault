package com.hardrock.games.pipeline;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hardrock.games.connectors.ConnectionPoolProvider;
import com.hardrock.games.model.constant.EventName;
import com.hardrock.games.model.input.InputPubSubMessage;
import com.hardrock.games.model.input.events.PhoneEventMessage;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import javax.sql.DataSource;

import static com.hardrock.games.helpers.HelperFunctions.GetEventName;

public class App {
    public interface Options extends StreamingOptions {
        @Description("Input text to print.")
        @Default.String("My input text")
        String getInputText();

        void setInputText(String value);
    }

    public static PCollection<String> buildPipeline(Pipeline pipeline, String inputText) {
        return pipeline
                .apply("Create elements", Create.of(Arrays.asList("Hello", "World!", inputText)))
                .apply("Print elements",
                        MapElements.into(TypeDescriptors.strings()).via(x -> {
                            System.out.println(x);
                            return x;
                        }));
    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);


//        String subscription = "projects/staging-204519/subscriptions/stg-datavault-poc-sub";


        // reading purchases
//        PCollectionTuple collectionPurchases = p
//
//                .apply("[INPUT]: Reading PubSub Purchases",
//                        PubsubIO.readStrings().fromSubscription(options.getSubscriptionPurchases()))
//                .apply("[INPUT]: String -> Purchase", ParDo.of(new DoFn<String, Purchase>() {
//                    Counter counterInputPurchasesOK = Metrics.counter("spend_level", "input_purchases_ok");
//                    Counter counterInputPurchasesFailed = Metrics.counter("spend_level", "input_purchases_failed");
//
//                    Counter totalPurchasesCountCounter = Metrics.counter("spend_level", "total_purchases_count");
//                    Counter totalPurchasesValueCounter = Metrics.counter("spend_level", "total_purchases_value");
//
//                    @ProcessElement
//                    public void processElement(ProcessContext c, OutputReceiver<Purchase> out) {
//                        String json = c.element();
//                        try {
//                            counterInputPurchasesOK.inc();
//                            Purchase res = PurchaseParser.parseJsonPurchases(json);
//                            Long spendCents = res.getRealMoneyPriceInCents();
//                            if (spendCents > 0 && res.getStatus().equals("success") && !isTestUser(res.getUid())) {
//                                totalPurchasesCountCounter.inc();
//                                totalPurchasesValueCounter.inc(spendCents);
//                            }
//                            out.output(res);
//                        } catch (Throwable throwable) {
//                            Failure failure = new Failure(json, throwable);
//                            LOG.info("[PUBSUB INPUT][PURCHASE][ERROR][Message=%s]".formatted(
//                                    failure.toJson()));
//                            counterInputPurchasesFailed.inc();
//                            c.output(failuresTag, failure);
//                        }
//                    }
//                }).withOutputTags(
//                        PurchaseParser.validTagPurchase,
//                        TupleTagList.of(failuresTag)));




        String subscription = "projects/prod-205719/subscriptions/prod-datavault-poc-sub";
                PCollection<PubsubMessage> inpPipeline =  pipeline
                        .apply("ReadFromPubSub", PubsubIO.readMessagesWithAttributesAndMessageId().fromSubscription(subscription));
//                        .apply("PrintMessages", ParDo.of(new DoFn<PubsubMessage, InputPubSubMessage>() {
//                            @ProcessElement
//                            public void processElement(ProcessContext context,OutputReceiver<InputPubSubMessage> out) throws JsonProcessingException, NoSuchAlgorithmException {
//
//                                PubsubMessage message = context.element();
//
//                                InputPubSubMessage inp = new InputPubSubMessage(message);
//
//                                System.out.println("Received message: " + message.getMessageId());
//                                out.output(inp);
//                            }
//                        }));
//
//        PCollection<PhoneEventMessage> jsonSweepstakesEligible =
//                inpPipeline.apply("Sweepstakes eligible",
//                        Filter.by((SerializableFunction<PubsubMessage, Boolean>) msg -> {
//                            try {
//                                if (GetEventName(msg) == EventName.UNITY_LOGIN) {
//                                    return new PhoneEventMessage(msg);
//                                }
//                            } catch (JsonProcessingException e) {
//                                throw new RuntimeException(e);
//                            } catch (NoSuchAlgorithmException e) {
//                                throw new RuntimeException(e);
//                            }
//                        } ));



//        inpPipeline.apply("[PROCESSING]: Update UserState",
//                ParDo.of(new DoFn<InputPubSubMessage, InputPubSubMessage>() {
//
//
//
//                             // mysql data source
//                             private DataSource dataSource;
//
//                             @StartBundle
//                             public void initializeConnection(PipelineOptions opt) {
//
////                                 SpendLevelRealtimeOptions myoptions = opt.as(SpendLevelRealtimeOptions.class);
//
////                                 LOG.info("[INITIALIZE_STATE]");
//                                 dataSource = ConnectionPoolProvider.getInstance(
//                                         "postgres",
//                                         "prod-205719:us-central1:bi-prod-db-01",
//                                         "postgres",
//                                         "Mzc1OTI1Y2MyMGJlNTVhYQo",
//                                         10,
//                                         20
//
//                                 ).getDataSource();
////
////                                 sql_cache_table = myoptions.getCloudSqlTable();
////                                 sql_cache_db = myoptions.getCloudSqlDb();
//
//
//
//                             }
//
//                             @ProcessElement
//                             public void processElement(
//                                     ProcessContext context,
//                                     @Element InputPubSubMessage message,
//
//                                     OutputReceiver<InputPubSubMessage> receiver) throws Exception {
//
//
//                                 try {
//
//
////                                     String query = "insert into stg.hub_uid values ('%s','%s','%s') ON CONFLICT (hkey) DO NOTHING;\ninsert into stg.hub_email values ('%s','%s','%s') ON CONFLICT (hkey) DO NOTHING;"
////                                     .formatted(message.getHkey_uid(),message.getUid(),"some_src",message.getHkey_email(),message.getEmail(),"some_src");
//////                                     String query1 = "insert into stg.hub_email values ('%s','%s','%s') ON CONFLICT (hkey) DO NOTHING".formatted(message.getEmail(),message.getEmail(),"some_src");
//
//
////
////                                     Connection con = dataSource.getConnection();
////                                     ResultSet rs = con.prepareStatement(query).executeQuery();
////
////                                     if (rs.next()) {
////                                         System.out.println(query);
////
////
////                                     } else {
////
////                                     }
////
////                                     con.close();
//
//
//
//                                     receiver.output(message);
//
//                                 } catch (Throwable throwable) {
//                                     System.out.println(throwable.getMessage());
//
//                                 }
//                             }
//
//                         }
//
//
//                        )
//                        );




                //hub_uid
//        inpPipeline.apply(JdbcIO.<InputPubSubMessage>write()
//                                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
//                                                "org.postgresql.Driver", "jdbc:postgresql://bi-prod-db-01.prod.wginfra.net:5432/postgres") //jdbc:postgresql://host:port/database
//                                        .withUsername("postgres")
//                                        .withPassword("Mzc1OTI1Y2MyMGJlNTVhYQo"))
//                                .withStatement("insert into stg.hub_uid values(?, ?, ?) ON CONFLICT (hkey) DO NOTHING")
//
//                                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<InputPubSubMessage>() {
//                                    public void setParameters(InputPubSubMessage element, PreparedStatement query)
//                                            throws SQLException {
//                                        query.setBytes(1, element.getHkey_uid());
//                                        query.setString(2, element.getUid());
//                                        query.setString(3, "src");
//                                    }
//                                })
//                        .withStatement("insert into stg.hub_email values(?, ?, ?) ON CONFLICT (hkey) DO NOTHING")
//                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<InputPubSubMessage>() {
//                            public void setParameters(InputPubSubMessage element, PreparedStatement query)
//                                    throws SQLException {
//                                query.setBytes(1, element.getHkey_email());
//                                query.setString(2, element.getEmail());
//                                query.setString(3, "src");
//                            }
//                        })
//                        .withStatement("insert into stg.link_uid_email values(?, ?, ?, ?) ON CONFLICT (hkey) DO NOTHING")
//                        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<InputPubSubMessage>() {
//                            public void setParameters(InputPubSubMessage element, PreparedStatement query)
//                                    throws SQLException {
//                                query.setBytes(1, element.getHkey_link());
//                                query.setBytes(2, element.getHkey_uid());
//                                query.setBytes(3, element.getHkey_email());
//                                query.setString(4, "src");
//                            }
//                        })
//                        );

//        inpPipeline.apply(JdbcIO.<InputPubSubMessage>write()
//                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
//                                "org.postgresql.Driver", "jdbc:postgresql://bi-prod-db-01.prod.wginfra.net:5432/postgres") //jdbc:postgresql://host:port/database
//                        .withUsername("postgres")
//                        .withPassword("Mzc1OTI1Y2MyMGJlNTVhYQo"))
//                .withStatement("insert into stg.hub_email values(?, ?, ?) ON CONFLICT (hkey) DO NOTHING")
//                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<InputPubSubMessage>() {
//                    public void setParameters(InputPubSubMessage element, PreparedStatement query)
//                            throws SQLException {
//                        query.setBytes(1, element.getHkey_email());
//                        query.setString(2, element.getEmail());
//                        query.setString(3, "src");
//                    }
//                })
//        );
//
//        inpPipeline.apply(JdbcIO.<InputPubSubMessage>write()
//                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
//                                "org.postgresql.Driver", "jdbc:postgresql://bi-prod-db-01.prod.wginfra.net:5432/postgres") //jdbc:postgresql://host:port/database
//                        .withUsername("postgres")
//                        .withPassword("Mzc1OTI1Y2MyMGJlNTVhYQo"))
//                .withStatement("insert into stg.link_uid_email values(?, ?, ?, ?) ON CONFLICT (hkey) DO NOTHING")
//                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<InputPubSubMessage>() {
//                    public void setParameters(InputPubSubMessage element, PreparedStatement query)
//                            throws SQLException {
//                        query.setString(1, element.getHkey_link());
//                        query.setString(2, element.getHkey_uid());
//                        query.setString(3, element.getHkey_email());
//                        query.setString(4, "src");
//                    }
//                })
//
//        );





//                        .apply(JdbcIO.<TableRow>write()
//                                .withDataSourceConfiguration(configurationMySQL)
//                                .withStatement(
//                                        """
//                                                REPLACE INTO %s
//                                                (uid,is_jp,purchase_count,total_before,total_after,spend_lvl,is_unity_player,tier_name,transactions)
//                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
//                                                """
//                                                .formatted(options.getCloudSqlTable()))
//                                .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<TableRow>) (elem, query) -> {
//                                    String uid = RowParserHelper.getStringValue(elem, "uid", false);
//                                    boolean isJp = isJP(uid);
//                                    int purchaseCount = RowParserHelper.getIntValue(elem, "purchase_count");
//                                    double totalBefore = RowParserHelper.getDoubleValue(elem, "total_before");
//                                    double total_after = RowParserHelper.getDoubleValue(elem, "total_after");
//                                    Long spend_lvl = RowParserHelper.getLongValue(elem, "spend_lvl");
//                                    boolean isunity = (Boolean) elem.getOrDefault("is_unity_player", false);
//                                    String unityTier = UnityTiersEnum
//                                            .parseUnityTiersEnum(RowParserHelper.getStringValue(elem, "tier_name", true))
//                                            .name();
//                                    String transactions = RowParserHelper.getStringValue(elem, "transactions",true);
//
//                                    query.setString(1, uid);
//                                    query.setBoolean(2, isJp);
//                                    query.setLong(3, purchaseCount);
//                                    query.setDouble(4, totalBefore);
//                                    query.setDouble(5, total_after);
//                                    query.setLong(6, spend_lvl);
//                                    query.setBoolean(7, isunity);
//                                    query.setString(8, unityTier);
//                                    query.setString(9, transactions);
//
//                                }).withResults());

                pipeline.run().waitUntilFinish();

    }
}











//public class App {
//
//    public interface Options extends StreamingOptions {
//        @Description("Input text to print.")
//        @Default.String("My input text")
//        String getInputText();
//
//        void setInputText(String value);
//    }
//
//    public static PCollection<String> buildPipeline(Pipeline pipeline, String inputText) {
//        return pipeline
//                .apply("Create elements", Create.of(Arrays.asList("Hello", "World!", inputText)))
//                .apply("Print elements",
//                        MapElements.into(TypeDescriptors.strings()).via(x -> {
//                            System.out.println(x);
//                            return x;
//                        }));
//    }
//
//
////    public static void main(String[] args) {
////        // Опции pipeline
////        PipelineOptions options = PipelineOptionsFactory
////                .as(StreamingOptions.class);
////
////        options.as(StreamingOptions.class).setStreaming(true); // Включаем потоковый режим
////
////        Pipeline pipeline = Pipeline.create(options);
////
////
////
////        String subscription = "projects/staging-204519/subscriptions/stg-datavault-poc-sub";
////
////
////        pipeline
////                .apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription(subscription))
////                .apply("PrintMessages", ParDo.of(new DoFn<String, Void>() {
////                    @ProcessElement
////                    public void processElement(ProcessContext context) {
////                        String message = context.element();
////                        System.out.println("Received message: " + message);
////                    }
////                }));
////
////        pipeline.run().waitUntilFinish();
////    }
//
//
//    public static void main(String[] args) {
//        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//        var pipeline = Pipeline.create(options);
//        App.buildPipeline(pipeline, options.getInputText());
//        pipeline.run().waitUntilFinish();
//    }
//}
