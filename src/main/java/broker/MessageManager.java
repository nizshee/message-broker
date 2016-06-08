package broker;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    public static final String TABLE_MSG = "msg";
    public static final String CREATE_MSG = "CREATE TABLE IF NOT EXISTS " + TABLE_MSG
            + "  (tId           INTEGER,"
            + "   num           INTEGER,"
            + "   msg           VARCHAR(128))";
    public static final String SELECT_ALL_MSG = "SELECT * FROM " + TABLE_MSG;
    public static final String SELECT_COUNT = "SELECT COUNT(1) FROM " + TABLE_MSG + " WHERE tId = ";
    public static final String INSERT_MSG = "INSERT INTO " + TABLE_MSG + " VALUES ";

    public MessageManager(Broker broker) throws Exception {
        this.broker = broker;

        msgCount = new ConcurrentHashMap<>();
        messages = new ConcurrentHashMap<>();

        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:" + broker.getFolder().getAbsolutePath() + "/msg", "broker", "");

        Statement stmt = conn.createStatement();
        stmt.execute(CREATE_MSG);

        ResultSet rs = stmt.executeQuery(SELECT_ALL_MSG);
        while (rs.next()) {
            int tId = rs.getInt("tId");
            String msg = rs.getString("msg");
            Topic topic = new Topic(tId);
            msgCount.putIfAbsent(topic, new AtomicInteger(0));
            msgCount.get(topic).incrementAndGet();
            messages.putIfAbsent(topic, new CopyOnWriteArrayList<>());
            messages.get(topic).add(new Message(msg));
        }
    }

    public void close() throws Exception {
        conn.close();
    }

    public int getMessageCount(Topic topic) {
        if (msgCount.containsKey(topic)) return msgCount.get(topic).get();
        return 0;
    }

    public void addMessage(Topic topic, Message message) throws Exception {

        messages.putIfAbsent(topic, new CopyOnWriteArrayList<>());
        msgCount.putIfAbsent(topic, new AtomicInteger(0));
        List<Message> list = messages.get(topic);
        synchronized (list) {
            AtomicInteger counter = msgCount.get(topic);
            int nSize = counter.incrementAndGet();

            Statement stmt = conn.createStatement();
            stmt.execute(INSERT_MSG + "(" + topic.id + ", " + nSize + ", \'" + message.text + "\')");
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
