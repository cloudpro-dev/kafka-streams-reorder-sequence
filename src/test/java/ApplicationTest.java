import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.Application;
import org.example.StreamsUtils;
import org.example.avro.ElectronicOrder;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ApplicationTest {

    @Test
    public void shouldReorderTheInput() {
        //
        // Step 1: Configure and start the processor topology.
        //
        final StreamsBuilder builder = new StreamsBuilder();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put("schema.registry.url", "mock://reorder-integration-test");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "reorder-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");

        final Map<String, Object> configMap =
                StreamsUtils.propertiesToMap(streamsConfiguration);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde =
                StreamsUtils.getSpecificAvroSerde(configMap);

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, electronicSerde.getClass().getName());

        final String inputTopic = "inputTopic";
        final String outputTopic = "outputTopic";
        final String reorderStore = "reorderStore";
        final String watermarkStore = "watermarkStore";

        final StoreBuilder<KeyValueStore<String, ElectronicOrder>> orderStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(reorderStore),
                        Serdes.String(),
                        electronicSerde);

        final StoreBuilder<KeyValueStore<String, Long>> watermarkStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(watermarkStore),
                        Serdes.String(),
                        Serdes.Long());

        builder.addStateStore(orderStoreSupplier);
        builder.addStateStore(watermarkStoreSupplier);

        final KStream<String, ElectronicOrder> stream = builder.stream(inputTopic);
        final KStream<String, ElectronicOrder> reordered = stream
                .process(new Application.ReorderProcessorSupplier(
                        reorderStore,
                        watermarkStore,
                        ElectronicOrder::getUserId,
                        ElectronicOrder::getSequenceNumber
                        ),
                        reorderStore,
                        watermarkStore);
        reordered.to(outputTopic);

        final Topology topology = builder.build();

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)) {
            //
            // Step 2: Setup input and output topics.
            //
            final TestInputTopic<String, ElectronicOrder> input = topologyTestDriver
                    .createInputTopic(inputTopic,
                            new Serdes.StringSerde().serializer(),
                            electronicSerde.serializer());

            final TestOutputTopic<String, ElectronicOrder> output = topologyTestDriver
                    .createOutputTopic(outputTopic,
                            new Serdes.StringSerde().deserializer(),
                            electronicSerde.deserializer());

            //
            // Step 3: Produce some input data to the input topic.
            //
            Fixtures.inputValues().forEach(order -> input.pipeInput(order.getUserId(), order));

            //
            // Step 4: Verify the application's output data.
            //
            assertThat(output.readValuesToList(), equalTo(Fixtures.expectedValues()));
        }
    }

}
