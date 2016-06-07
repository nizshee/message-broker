package broker;

import org.junit.Test;

import static org.junit.Assert.*;

public class BrokerTest {

    @Test
    public void constructorTest() throws Exception {

        Broker broker = new Broker("/tmp/test1");
        assertTrue(true);
        broker.subscribe(new Subscriber(0), new Topic("0"));
        broker.subscribe(new Subscriber(2), new Topic("0"));
        broker.subscribe(new Subscriber(2), new Topic("1"));
        broker.publish(new Topic("0"), new Message("abc"));
        broker.publish(new Topic("0"), new Message("ab"));
        System.err.println(broker.fetch(new Subscriber(0)).get().text);
        System.err.println(broker.fetch(new Subscriber(2)).get().text);
        System.err.println(broker.fetch(new Subscriber(2)).get().text);
        System.err.println(broker.fetch(new Subscriber(2)));
        broker.close();

    }
}
