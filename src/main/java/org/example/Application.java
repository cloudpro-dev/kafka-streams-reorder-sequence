package org.example;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.String;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.example.avro.ElectronicOrder;

import static org.example.StreamsUtils.*;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    /**
     * Kafka streams processor which copies records from one topic to another, reordering them based upon the
     * <code>sequence_number</code> embedded in the record payload.
     */
    public static class ReorderProcessorSupplier implements ProcessorSupplier<String, ElectronicOrder, String, ElectronicOrder> {

        public interface StoreKeyGenerator {
            String getStoreKey(ElectronicOrder value);
        }

        public interface SequenceNumberGenerator {
            Long getSequenceNumber(ElectronicOrder value);
        }

        private final String reorderStoreName;

        private final String watermarkStoreName;

        private final StoreKeyGenerator storeKeyGenerator;

        private final SequenceNumberGenerator sequenceNumberGenerator;

        public ReorderProcessorSupplier(String reorderStoreName, String watermarkStoreName, StoreKeyGenerator storeKeyGenerator, SequenceNumberGenerator sequenceNumberGenerator) {
            this.reorderStoreName = reorderStoreName;
            this.watermarkStoreName = watermarkStoreName;
            this.storeKeyGenerator = storeKeyGenerator;
            this.sequenceNumberGenerator = sequenceNumberGenerator;
        }

        @Override
        public Processor<String, ElectronicOrder, String, ElectronicOrder> get() {
            return new Processor<String, ElectronicOrder, String, ElectronicOrder>() {

                private KeyValueStore<String, ElectronicOrder> reorderStore;
                private KeyValueStore<String, Long> watermarkStore;
                private ProcessorContext<String, ElectronicOrder> context;

                @Override
                public void init(ProcessorContext<String, ElectronicOrder> context) {
                    reorderStore = context.getStateStore(reorderStoreName);
                    watermarkStore = context.getStateStore(watermarkStoreName);
                    this.context = context;
                }

                /**
                 * Process incoming records to the input topic.
                 * <p></p>
                 * The incoming record is stored in the <code>reorderStore</code> and the <code>sequence_number</code>
                 * is read. If the record <code>sequence_number</code> is exactly one more than the value of the
                 * <code>watermark</code> for the corresponding key, the record to be forwarded to the output topic.
                 *
                 * @param record the incoming record
                 */
                @Override
                public void process(Record<String, ElectronicOrder> record) {
                    // get the sequence number from the record payload
                    final Long recSeq = sequenceNumberGenerator.getSequenceNumber(record.value());
                    final String recKey = storeKeyGenerator.getStoreKey(record.value());

                    // store the record - using sequence number as key to ensure they are ordered
                    reorderStore.put(String.valueOf(recSeq), record.value());
                    logger.info("Added record - key " + record.key() + " value " + record.value() + " to store");

                    // get current watermark value
                    Long watermark = watermarkStore.get(recKey);
                    if(watermark == null) {
                        watermark = 0L;
                        watermarkStore.put(recKey, watermark);
                    }

                    // if record is the next in the sequence, then forward immediately
                    if(watermark+1 == recSeq) {
                        logger.info("Forwarding record");
                        forward(Instant.now().toEpochMilli());
                    }
                }

                /**
                 * Forward any cached records from the <code>reorderStore</code> that make up a contiguous sequence
                 * based upon the current value of the <code>watermark</code> for the corresponding key.
                 *
                 * @param timestamp timestamp to use when writing new record to the output topic
                 */
                void forward(long timestamp) {
                    try(KeyValueIterator<String, ElectronicOrder> it = reorderStore.all()) {
                        // Iterate over the records and create a Record instance and forward downstream
                        while(it.hasNext()) {
                            final KeyValue<String, ElectronicOrder> kv = it.next();
                            final String recKey = storeKeyGenerator.getStoreKey(kv.value);
                            final Long recSeq = sequenceNumberGenerator.getSequenceNumber(kv.value);
                            // use partition key to get watermark
                            final Long watermark = watermarkStore.get(recKey);

                            // if the record sequence number is the next in the sequence, based upon the watermark
                            if(watermark+1 == recSeq) {
                                // build a new record
                                Record<String, ElectronicOrder> rec = new Record<>(recKey, kv.value, timestamp);
                                // send the new record to the output topic
                                context.forward(rec);
                                logger.info("Sent new record - key " + rec.key() + " value " + rec.value());
                                // remove the old record from the store
                                reorderStore.delete(kv.key);
                                // update the watermark for the corresponding key
                                watermarkStore.put(recKey, watermark+1);
                                logger.info("Watermark is now " + watermark);
                            }
                        }
                    }
                }
            };
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
        }

        final StreamsBuilder builder = new StreamsBuilder();

        final Properties streamsProps = loadProperties(args[0]);
        final Map<String, Object> configMap = propertiesToMap(streamsProps);

        final Serde<String> stringSerde = Serdes.String();
        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);

        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "reorder-api-application");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, electronicSerde.getClass().getName());

        final String inputTopic = streamsProps.getProperty("input.topic.name");
        final String outputTopic = streamsProps.getProperty("output.topic.name");
        final String persistentStore = "reorderStore";
        final String watermarkStore = "watermarkStore";

        final StoreBuilder<KeyValueStore<String, ElectronicOrder>> orderStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(persistentStore),
                        stringSerde,
                        electronicSerde);

        final StoreBuilder<KeyValueStore<String, Long>> watermarkStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(watermarkStore),
                        Serdes.String(),
                        Serdes.Long());

        builder.addStateStore(watermarkStoreSupplier);
        builder.addStateStore(orderStoreSupplier);

        final KStream<String, ElectronicOrder> stream = builder.stream(inputTopic);
        final KStream<String, ElectronicOrder> reordered = stream
                .process(new ReorderProcessorSupplier(
                        persistentStore, watermarkStore,
                        ElectronicOrder::getUserId,
                        ElectronicOrder::getSequenceNumber
                ), persistentStore, watermarkStore);
        reordered.to(outputTopic);

        final Topology topology = builder.build();

        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));

            // Clean local state prior to starting the processing topology.
            kafkaStreams.cleanUp();

            try {
                logger.info("Starting application");
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}