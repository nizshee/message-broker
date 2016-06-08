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


    public Broker(String folderName) throws Exception {

        folder = new File(folderName);
        folder.mkdirs();

        topics = new TopicManager(this);
        subscribers = new SubscriberManager(this);
        messages = new MessageManager(this);
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

    public void publish(Topic topic, Message message) throws Exception {
        topics.publish(topic, message);
    }

    public Optional<Message> fetch(Subscriber subscriber) throws Exception {
        return subscribers.getNewMessage(subscriber);
    }

    public void addMessage(Topic topic, Message message) throws Exception {
        messages.addMessage(topic, message);
    }

    public Optional<Message> getMessage(Topic topic, int number) {
        return messages.getMessage(topic, number);
    }

    public int getMessageCount(Topic topic) {
        return messages.getMessageCount(topic);
    }

}
