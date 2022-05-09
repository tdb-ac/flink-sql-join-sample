package be.flink.sql.join.sample.io;

import org.apache.commons.lang3.StringUtils;

class PulsarConnection {

    private static final String ADMIN_URL = "http://%s:%s";
    private static final String SERVICE_URL = "pulsar://%s:%s";
    private static final String TOPIC = "persistent://%s/%s/%s";

    protected String brokerHost;
    protected String brokerServicePort;
    protected String brokerAdminPort;
    protected String tenant;
    protected String nameSpace;
    protected String topic;

    PulsarConnection() {
        // Package private use
    }

    void setHost(String brokerHost, String brokerServicePort, String brokerAdminPort) {
        this.brokerHost = brokerHost == null ? "pulsar" : brokerHost;
        this.brokerServicePort = brokerServicePort == null ? "6650" : brokerServicePort;
        this.brokerAdminPort = brokerAdminPort == null ? "8090" : brokerAdminPort;
    }

    void setNameSpace(String tenant, String nameSpace) {
        this.tenant = tenant;
        this.nameSpace = nameSpace;
    }

    void setTopic(String topic) {
        this.topic = topic;
    }

    String getAdminUrl() {
        return String.format(ADMIN_URL, brokerHost, brokerAdminPort);
    }

    String getServiceUrl() {
        return String.format(SERVICE_URL, brokerHost, brokerServicePort);
    }

    String getFullTopic() {
        return String.format(TOPIC, tenant, nameSpace, topic);
    }

    void validateConnectionDetails() {
        if (StringUtils.isAnyEmpty(brokerHost, brokerAdminPort, brokerServicePort, tenant, nameSpace, topic)) {
            throw new IllegalArgumentException("Please provide required details for this Pulsar Source using the builder methods");
        }
    }

}
