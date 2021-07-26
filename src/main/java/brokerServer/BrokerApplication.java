package brokerServer;

public class BrokerApplication {

    public static void main(String[] args) throws Exception {
        BrokerServer brokerServer =new BrokerServer("127.0.0.1",8888);

        brokerServer.start();
    }
}

