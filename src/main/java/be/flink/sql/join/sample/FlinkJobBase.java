package be.flink.sql.join.sample;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Properties;

public abstract class FlinkJobBase {

    protected long DEFAULT_CHECKPOINT_INTERVAL = 5000L;
    protected StreamExecutionEnvironment environment;
    protected StreamTableEnvironment tableEnvironment;
    protected String brokerHost;
    protected String brokerServicePort;
    protected String brokerAdminPort;
    protected String pulsarTenant;
    protected String pulsarNamespace;
    protected String pulsarSourceDbName;
    protected String pulsarSourceDbSchema;
    protected String runId;

    protected <T> FlinkJobBase(long checkpointInterval, Class<T> flinkJobClass, String[] args) throws IOException {
        final Properties properties = new Properties();
        properties.load(flinkJobClass.getClassLoader().getResourceAsStream("project.properties"));
        String jobName = properties.getProperty("artifactId") + " " + properties.getProperty("version");
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(checkpointInterval);
        tableEnvironment = StreamTableEnvironment.create(environment);
        Configuration configuration = tableEnvironment.getConfig().getConfiguration();
        configuration.setString("pipeline.name", jobName);
        extractJobArguments(args);
    }

    protected void extractJobArguments(String[] args) {
        ParameterTool paramTool = ParameterTool.fromArgs(args);
        runId = paramTool.get("RUN_ID", "");
        brokerHost = paramTool.get("PULSAR_BROKER_HOST", "pulsar");
        brokerServicePort = paramTool.get("PULSAR_BROKER_SERVICE_PORT", "6650");
        brokerAdminPort = paramTool.get("PULSAR_BROKER_ADMIN_PORT", "8090");
        pulsarTenant = paramTool.get("PULSAR_TENANT", "connect-evolution");
        pulsarNamespace = paramTool.get("PULSAR_NAMESPACE", "calendar-acl");
        pulsarSourceDbName = paramTool.get("PULSAR_SOURCE_DB_NAME", "cdc");
        pulsarSourceDbSchema = paramTool.get("PULSAR_SOURCE_DB_SCHEMA", "SCHEMA");
    }

}
