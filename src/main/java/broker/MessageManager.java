package broker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class MessageManager {

    private final Connection conn;

    private final ConcurrentHashMap<Topic, List<Message>> messages;

    public static final String TABLE_MSG = "msg";
    public static final String CREATE_MSG = "CREATE TABLE IF NOT EXISTS " + TABLE_MSG
            + "  (tId           INTEGER,"
            + "   num           INTEGER,"
            + "   msg           VARCHAR(128))";
    public static final String SELECT_ALL_MSG = "SELECT * FROM " + TABLE_MSG + " ORDER BY num ASC";
    public static final String INSERT_MSG = "INSERT INTO " + TABLE_MSG + " VALUES ";

    public MessageManager(Broker broker) throws Exception {

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
            messages.putIfAbsent(topic, new CopyOnWriteArrayList<>());
            messages.get(topic).add(new Message(msg));
        }
    }

    public void close() throws Exception {
        conn.close();
    }

    public int getMessageCount(Topic topic) {
        if (messages.containsKey(topic)) return messages.get(topic).size();
        return 0;
    }

    public void addMessage(Topic topic, Message message) throws Exception {

        messages.putIfAbsent(topic, new CopyOnWriteArrayList<>());
        List<Message> list = messages.get(topic);
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (list) {
            int nSize = list.size();

            Statement stmt = conn.createStatement();
            stmt.execute(INSERT_MSG + "(" + topic.id + ", " + nSize + ", \'" + message.text + "\')");
            list.add(message);
        }

    }

    @SuppressWarnings("unchecked")
    public Optional<Message> getMessage(Topic topic, int number) {
        if (number <= 0 || number > messages.getOrDefault(topic, Collections.EMPTY_LIST).size())
            return Optional.empty();
        return Optional.of(messages.get(topic).get(number - 1));
    }
}
