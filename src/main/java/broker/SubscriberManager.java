package broker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;


public class SubscriberManager {

    private final Broker broker;
    private final Connection conn;

    private final ConcurrentMap<Subscriber, ConcurrentMap<Topic, AtomicInteger>> state;

    public static final String TABLE = "subs";
    public static final String CREATE = "CREATE TABLE IF NOT EXISTS " + TABLE
            + "  (sId           INTEGER,"
            + "   tId           INTEGER,"
            + "   num           INTEGER)";
    public static final String SELECT_ALL = "SELECT * FROM " + TABLE;
    public static final String INSERT = "INSERT INTO " + TABLE + " VALUES ";
    public static final String UPDATE = "UPDATE " + TABLE + " SET ";

    public SubscriberManager(Broker broker) throws Exception {

        this.broker = broker;

        state = new ConcurrentHashMap<>();
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:" + broker.getFolder().getAbsolutePath() + "/subs", "broker", "");

        Statement stmt = conn.createStatement();
        stmt.execute(CREATE);
        ResultSet rs = stmt.executeQuery(SELECT_ALL);
        while (rs.next()) {
            int sId = rs.getInt("sId");
            int tId = rs.getInt("tId");
            int num = rs.getInt("num");
            Subscriber subscriber = new Subscriber(sId);
            Topic topic = new Topic(tId);
            state.putIfAbsent(subscriber, new ConcurrentHashMap<>());
            ConcurrentMap<Topic, AtomicInteger> map = state.get(subscriber);
            map.put(topic, new AtomicInteger(num));
        }
    }

    public void close() throws Exception {
        conn.close();
    }

    public void subscribe(Subscriber subscriber, Topic topic) throws Exception {
        int count = broker.addSubscriber(topic, subscriber);

        state.putIfAbsent(subscriber, new ConcurrentHashMap<>());
        ConcurrentMap<Topic, AtomicInteger> map = state.get(subscriber);
        if (map.containsKey(topic)) return;
        map.put(topic, new AtomicInteger(count));

        Statement stmt = conn.createStatement();
        stmt.execute(INSERT + "(" + subscriber.id + ", " + topic.id + ", " + count + ")");
    }

    public Optional<Message> getNewMessage(Subscriber subscriber) throws Exception {
        if (!state.containsKey(subscriber)) return Optional.empty();
        ConcurrentMap<Topic, AtomicInteger> map = state.get(subscriber);

        Optional<Topic> oTopic;
        int nSize = 0;
        synchronized (map) {
            oTopic = getChangedTopic(subscriber);
            if (oTopic.isPresent()) {
                nSize = map.get(oTopic.get()).incrementAndGet();

                Statement stmt = conn.createStatement();
                stmt.execute(UPDATE + "num = " + nSize + " WHERE sId = " + subscriber.id
                        + " AND tId = " + oTopic.get().id + ";");
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
