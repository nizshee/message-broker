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

        Optional<Topic> oTopic;
        int nSize = 0;
        synchronized (map) {
            oTopic = getChangedTopic(subscriber);
            if (oTopic.isPresent()) {
                nSize = map.get(oTopic.get()).incrementAndGet();
                // TODO save nSize
            }
        }
        if (oTopic.isPresent()) return broker.getMessage(oTopic.get(), nSize);
        return Optional.empty();

    }

    private Optional<Topic> getChangedTopic(Subscriber subscriber) {
        if (!state.containsKey(subscriber)) return Optional.empty();
        ConcurrentMap<Topic, AtomicInteger> map = state.get(subscriber);
        for (Topic topic : map.keySet()) {
            if (map.get(topic).get() < broker.getMessageCount(topic)) return Optional.of(topic);
        }
        return Optional.empty();
    }

}
