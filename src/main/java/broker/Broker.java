package broker;


import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Broker {

    private final String name;

    private final TopicManager topics;
    private final SubscriberManager subscribers;
    private final MessageManager messages;

    private final ConcurrentMap<Subscriber, LinkedList<Object>> monitors;


    public void close() throws Exception {
    }

    public String getFolderName() {
        return name;
    }

    public int addSubscriber(Topic topic, Subscriber subscriber) {
        return topics.addSubscriber(topic, subscriber);
    }

    public void subscribe(Subscriber subscriber, Topic topic) {
        subscribers.subscribe(subscriber, topic);
    }

    public Broker(String folderName) throws Exception {

        name = folderName;

        topics = new TopicManager(this);
        subscribers = new SubscriberManager(this);
        messages = new MessageManager(this);

        monitors = new ConcurrentHashMap<>();
        //    private Connection conn = null;
//        Class.forName("org.h2.Driver");
//        conn = DriverManager.getConnection("jdbc:h2:" + folderName, "broker", "");
//        conn.close();
    }

    public void publish(Topic topic, Message message) {
        topics.publish(topic, message);
    }

    public Optional<Message> fetch(Subscriber subscriber) {
        return subscribers.getNewMessage(subscriber);
    }

    public void addMessage(Topic topic, Message message) {
        messages.addMessage(topic, message);
    }

    public Optional<Message> getMessage(Topic topic, int number) {
        return messages.getMessage(topic, number);
    }

    public int getMessageCount(Topic topic) {
        return messages.getMessageCount(topic);
    }

}
