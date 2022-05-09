package be.flink.sql.join.sample.io;

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalSource {

    private static final Map<String, String> DEFAULT_SOURCE_PROPERTIES = Map.ofEntries(
            Map.entry("format","debezium-json"),
            Map.entry("connector", "filesystem")
    );

    private final String tableName;

    public LocalSource(String tableName) {
        this.tableName = tableName.toLowerCase(Locale.ROOT) + ".txt";
    }

    private final Map<String, String> properties = new TreeMap<>(DEFAULT_SOURCE_PROPERTIES);

    public static LocalSource createForTable(String tableName) {
        return new LocalSource(tableName);
    }


    public String build() {
        properties.put("path", getClass().getClassLoader().getResource(tableName).getPath());

        return properties.entrySet().stream()
                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));

    }

}
