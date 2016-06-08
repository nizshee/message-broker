package broker;


import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageManager {

    private final Broker broker;
    private final Connection conn;

    private final ConcurrentMap<Topic, AtomicInteger> msgCount;
    private final ConcurrentHashMap<Topic, List<Message>> messages;

    public MessageManager(Broker broker) throws Exception {
        this.broker = broker;

        msgCount = new ConcurrentHashMap<>();
        messages = new ConcurrentHashMap<>();

        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:" + broker.getFolder().getAbsolutePath() + "/msg", "broker", "");

        // TODO init msgCount messages
    }

    public void close() throws Exception {

    }

    public int getMessageCount(Topic topic) {
        if (msgCount.containsKey(topic)) return msgCount.get(topic).get();
        return 0;
    }

    public void addMessage(Topic topic, Message message) {

        messages.putIfAbsent(topic, new CopyOnWriteArrayList<>());
        msgCount.putIfAbsent(topic, new AtomicInteger(0));
        List<Message> list = messages.get(topic);
        synchronized (list) {
            AtomicInteger counter = msgCount.get(topic);
            int nSize = counter.incrementAndGet();

            // TODO save message nSize
            list.add(message);
        }

    }

    public Optional<Message> getMessage(Topic topic, int number) {
        if (number <= 0 || number > messages.getOrDefault(topic, Collections.EMPTY_LIST).size())
            return Optional.empty();
        return Optional.of(messages.get(topic).get(number - 1));
    }
}
