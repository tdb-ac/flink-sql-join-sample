package be.flink.sql.join.sample.io;

import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.config.RecordSchemaType;
import org.apache.flink.streaming.connectors.pulsar.internal.JsonSer;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchema;
import org.apache.flink.streaming.util.serialization.PulsarSerializationSchemaWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class PulsarSink<T extends PulsarSerializable> {

    private static final Map<String, String> DEFAULT_SINK_PROPERTIES = Map.ofEntries(
            Map.entry("key.format", "json"),
            Map.entry("value.format", "json"),
            Map.entry("pulsar.producer.blockIfQueueFull", "true")
    );

    private final Class<T> clazz;
    private final PulsarConnection connection;
    private final Map<String, String> propertiesMap = new HashMap<>(DEFAULT_SINK_PROPERTIES);

    private PulsarSink(Class<T> clazz) {
        this.clazz = clazz;
        this.connection = new PulsarConnection();
    }

    public static <V extends PulsarSerializable> PulsarSink<V> createForClass(Class<V> clazz) {
        return new PulsarSink<>(clazz);
    }

    public PulsarSink<T> forHost(String brokerHost, String brokerServicePort, String brokerAdminPort) {
        connection.setHost(brokerHost, brokerServicePort, brokerAdminPort);
        return this;
    }

    public PulsarSink<T> forNameSpace(String tenant, String nameSpace) {
        connection.setNameSpace(tenant, nameSpace);
        return this;
    }

    public PulsarSink<T> forTopic(String topic) {
        connection.setTopic(topic);
        return this;
    }

    public PulsarSink<T> withExtraProperties(Map<String, String> extraProperties) {
        propertiesMap.putAll(extraProperties);
        return this;
    }

    public FlinkPulsarSink<T> build() {
        connection.validateConnectionDetails();
        PulsarSerializationSchema<T> pulsarSerialization = new PulsarSerializationSchemaWrapper
                .Builder<>(JsonSer.of(clazz))
                .usePojoMode(clazz, RecordSchemaType.JSON)
                .setKeyExtractor(T::getMessageKey)
                .build();
        Properties properties = new Properties();
        propertiesMap.forEach(properties::setProperty);
        return new FlinkPulsarSink<>(
                connection.getServiceUrl(),
                connection.getAdminUrl(),
                Optional.of(connection.getFullTopic()),
                properties,
                pulsarSerialization);
    }

}
