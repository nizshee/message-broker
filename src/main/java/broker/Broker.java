package broker;

import java.io.File;
import java.util.Optional;


public class Broker {

    private final File folder;

    private final SubscriberManager subscribers;
    private final MessageManager messages;


    public Broker(String folderName) throws Exception {

        folder = new File(folderName);
        //noinspection ResultOfMethodCallIgnored
        folder.mkdirs();

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



    public void subscribe(Subscriber subscriber, Topic topic) throws Exception {
        subscribers.addTopic(subscriber, topic);
    }

    public void publish(Topic topic, Message message) throws Exception {
        messages.addMessage(topic, message);
    }

    public Optional<Message> fetch(Subscriber subscriber) throws Exception {
        return subscribers.getNewMessage(subscriber);
    }

    public Optional<Message> getMessage(Topic topic, int number) {
        return messages.getMessage(topic, number);
    }

    public int getMessageCount(Topic topic) {
        return messages.getMessageCount(topic);
    }

}
