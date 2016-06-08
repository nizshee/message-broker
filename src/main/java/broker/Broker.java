package broker;


import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Broker {

    private final File folder;

    private final TopicManager topics;
    private final SubscriberManager subscribers;
    private final MessageManager messages;

    private final ConcurrentMap<Subscriber, LinkedList<Object>> monitors;


    public Broker(String folderName) throws Exception {

        folder = new File(folderName);
        folder.mkdirs();
        System.err.println(folder.getAbsolutePath());

        topics = new TopicManager(this);
        subscribers = new SubscriberManager(this);
        messages = new MessageManager(this);

        monitors = new ConcurrentHashMap<>();
        //    private Connection conn = null;
//        Class.forName("org.h2.Driver");
//        conn = DriverManager.getConnection("jdbc:h2:" + folderName, "broker", "");
//        conn.close();
    }

    public void close() throws Exception {
        subscribers.close();
        messages.close();
    }

    public File getFolder() {
        return folder;
    }

    public int addSubscriber(Topic topic, Subscriber subscriber) {
        return topics.addSubscriber(topic, subscriber);
    }

    public void subscribe(Subscriber subscriber, Topic topic) throws Exception {
        subscribers.subscribe(subscriber, topic);
    }

    public void publish(Topic topic, Message message) {
        topics.publish(topic, message);
    }

    public Optional<Message> fetch(Subscriber subscriber) throws Exception {
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
