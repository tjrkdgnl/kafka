package consumer;


import java.util.HashSet;
import java.util.Set;

public class SubscribeState {
    private  Set<String> subscription;


    public SubscribeState(){
        subscription =new HashSet<>();

    }

    public void setSubscription(Set<String> topics) {
        this.subscription = topics;
    }

    public String[] getSubscription() {
        return subscription.toArray(new String[subscription.size()-1]);
    }
}
