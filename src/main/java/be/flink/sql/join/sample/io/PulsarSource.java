package be.flink.sql.join.sample.io;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PulsarSource {

    private static final Map<String, String> DEFAULT_SOURCE_PROPERTIES = Map.ofEntries(
            Map.entry("scan.startup.mode","earliest"),
            Map.entry("value.format","debezium-json"),
            Map.entry("connector", "pulsar")
    );

    private final PulsarConnection connection;
    private final Map<String, String> properties = new TreeMap<>(DEFAULT_SOURCE_PROPERTIES);

    private PulsarSource() {
        this.connection = new PulsarConnection();
    }

    public static PulsarSource create() {
        return new PulsarSource();
    }

    public PulsarSource forHost(String brokerHost, String brokerServicePort, String brokerAdminPort) {
        connection.setHost(brokerHost, brokerServicePort, brokerAdminPort);
        return this;
    }

    public PulsarSource forNameSpace(String tenant, String nameSpace) {
        connection.setNameSpace(tenant, nameSpace);
        return this;
    }

    public PulsarSource forTopic(String dbName, String dbSchema, String tableName) {
        connection.setTopic(Stream.of(dbName, dbSchema, tableName)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.joining(".")));
        return this;
    }

    public PulsarSource forTopic(String topic) {
        connection.setTopic(topic);
        return this;
    }

    public PulsarSource withExtraProperties(Map<String, String> extraProperties) {
        properties.putAll(extraProperties);
        return this;
    }

    public String build() {
        connection.validateConnectionDetails();
        properties.put("topic", connection.getFullTopic());
        properties.put("service-url", connection.getServiceUrl());
        properties.put("admin-url", connection.getAdminUrl());

        return properties.entrySet().stream()
                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));

    }

}
