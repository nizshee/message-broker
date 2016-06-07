package broker;

import java.sql.*;


public class Message {

    public final String text;

    public Message(String text) {
        this.text = text;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Message && text.equals(((Message) o).text);
    }

    @Override
    public int hashCode() {
        return text.hashCode();
    }
}
