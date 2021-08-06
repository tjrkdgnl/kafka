package consumer;

public enum ConsumerConfig {
    SERVER("server_address"),GROUP_ID("group_id"),PORT("port");


    private final String value;

    ConsumerConfig(String value) {
        this.value =value;
    }

    public String getValue() {
        return value;
    }
}
