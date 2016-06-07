package broker;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;


public class SubscriberManager {

    private final Broker broker;

    private final ConcurrentMap<Subscriber, ConcurrentMap<Topic, AtomicInteger>> state;

    public SubscriberManager(Broker broker) {
        this.broker = broker;

        state = new ConcurrentHashMap<>();
        // TODO init
        // TODO addSubscriber
    }

    public void subscribe(Subscriber subscriber, Topic topic) {
        int count = broker.addSubscriber(topic, subscriber);
        // TODO save

        state.putIfAbsent(subscriber, new ConcurrentHashMap<>());
        ConcurrentMap<Topic, AtomicInteger> map = state.get(subscriber);
        map.putIfAbsent(topic, new AtomicInteger(count));
    }

    public Optional<Message> getNewMessage(Subscriber subscriber) {
        if (!state.containsKey(subscriber)) return Optional.empty();
        ConcurrentMap<Topic, AtomicInteger> map = state.get(subscriber);
        Set<Topic> set = map.keySet();
        for (Topic topic : set) {
            int count = broker.getMessageCount(topic);
            AtomicInteger current = map.get(topic);
            int curr = current.get();
            while (count >= curr) {
                if (current.compareAndSet(curr, curr + 1)) break;
                curr = current.get();
            }
            curr += 1;
            System.err.println(curr);
            return broker.getMessage(topic, curr);

        }
        return Optional.empty();
    }

}
