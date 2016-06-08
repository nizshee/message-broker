package broker;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class TopicManager {

    private final Broker broker;

    private final ConcurrentMap<Topic, List<Subscriber>> subscribers;

    public TopicManager(Broker broker) {
        this.broker = broker;
        subscribers = new ConcurrentHashMap<>();

        // TODO init
    }

    public int addSubscriber(Topic topic, Subscriber subscriber) {
        subscribers.putIfAbsent(topic, new CopyOnWriteArrayList<>());
        List<Subscriber> list = subscribers.get(topic);
        list.add(subscriber);
        return broker.getMessageCount(topic);
    }

    public void publish(Topic topic, Message message) throws Exception {
        broker.addMessage(topic, message);
        // TODO
    }
}
