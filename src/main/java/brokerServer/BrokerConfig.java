package brokerServer;


public enum BrokerConfig {
    ID("broker_id"),HOST("host"), PORT("port"),LOG_DIRS("log_dirs"),
    TOPIC_PARTITIONS("topic_partitions"),SEGMENT_BYTES("segrment_bytes"),
    AUTO_CREATE_TOPIC("auto_create_topics"),REPLICATION_FACTOR("replication_factor");

    private final String value;

    BrokerConfig(String value){
        this.value =value;
    }

    public String getValue() {
        return value;
    }
}
