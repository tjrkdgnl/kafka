package consumer;

public enum ConsumerConfig {
    SERVER("serverAddress"), GROUP_ID("groupId"), PORT("port"),
    CONSUMER_ID("consumerId"), SESSION_TIMEOUT("sessionTimeout"),
    HEARTBEAT_INTERVAL("heartbeatInterval");


    private final String value;

    ConsumerConfig(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
