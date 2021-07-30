package brokerServer;


public enum BrokerConfig {
    ID("broker_id"),HOST("host"), PORT("port"),
    TOPIC_PARTITIONS("topic_partitions"),SEGMENT_BYTES("segrment_bytes");

    private final String value;

    BrokerConfig(String value){
        this.value =value;
    }

    public String getValue() {
        return value;
    }
}
