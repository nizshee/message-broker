package broker;

import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class BrokerTest {

    public static final String DIRECTORY_NAME = "/tmp/test1";

    public static final Subscriber SUBS1 = new Subscriber(1);
    public static final Subscriber SUBS2 = new Subscriber(2);
    public static final Subscriber SUBS3 = new Subscriber(3);

    public static final Topic TOP1 = new Topic(1);
    public static final Topic TOP2 = new Topic(2);

    @SuppressWarnings("all")
    void delete(File f) {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        f.delete();
    }

    public void deleteDirectory() {
        File file = new File(DIRECTORY_NAME);
        delete(file);
    }

    @Test
    public void simpleTest() throws Exception {

        deleteDirectory();

        Broker broker = new Broker(DIRECTORY_NAME);

        broker.subscribe(SUBS1, TOP1);
        broker.subscribe(SUBS2, TOP1);
        broker.subscribe(SUBS2, TOP2);

        broker.publish(TOP1, new Message("abc"));
        Optional<Message> oMessage;

        oMessage = broker.fetch(SUBS1);
        assertTrue(oMessage.isPresent());
        assertEquals(oMessage.get().text, "abc");

        oMessage = broker.fetch(SUBS2);
        assertTrue(oMessage.isPresent());
        assertEquals(oMessage.get().text, "abc");

        broker.publish(TOP2, new Message("ab"));

        oMessage = broker.fetch(SUBS2);
        assertTrue(oMessage.isPresent());
        assertEquals(oMessage.get().text, "ab");

        oMessage = broker.fetch(SUBS1);
        assertFalse(oMessage.isPresent());

        oMessage = broker.fetch(SUBS2);
        assertFalse(oMessage.isPresent());

        broker.subscribe(SUBS3, TOP2);

        oMessage = broker.fetch(SUBS3);
        assertFalse(oMessage.isPresent());

        broker.publish(TOP2, new Message("a"));

        oMessage = broker.fetch(SUBS2);
        assertTrue(oMessage.isPresent());
        assertEquals(oMessage.get().text, "a");

        oMessage = broker.fetch(SUBS3);
        assertTrue(oMessage.isPresent());
        assertEquals(oMessage.get().text, "a");

        oMessage = broker.fetch(SUBS1);
        assertFalse(oMessage.isPresent());
        oMessage = broker.fetch(SUBS2);
        assertFalse(oMessage.isPresent());
        oMessage = broker.fetch(SUBS3);
        assertFalse(oMessage.isPresent());

        broker.close();
        deleteDirectory();
    }

    @Test
    public void concurrentTest() throws Exception {

        int threadCount = 8;
        int messageCount = 1000;

        deleteDirectory();

        Broker broker = new Broker(DIRECTORY_NAME);
        for (int i = 0; i < threadCount; ++i) {
            Subscriber subscriber = new Subscriber(i);
            for (int j = 0; j < 2; ++j) {
                broker.subscribe(subscriber, new Topic(j % threadCount));
            }
        }

        List<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread() {
            @Override
            public void run() {
                Topic topic1 = new Topic(i);
                Topic topic2 = new Topic(i);
                try {
                    for (int i = 0; i < messageCount; ++i) {
                        broker.publish(topic1, new Message("" + i));
                        broker.publish(topic2, new Message("" + i));
                    }
                } catch (Exception ignore) {
                }
            }
        }).collect(Collectors.toList());

        threads.forEach(Thread::start);
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (Exception ignore) {
            }
        });

        List<AtomicInteger> total = new CopyOnWriteArrayList<>(IntStream.range(0, threadCount)
                .mapToObj(i -> new AtomicInteger(0)).collect(Collectors.toList()));
        threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread() {
            @Override
            public void run() {
                int j = (i + 1) % threadCount;
                Subscriber subscriber1 = new Subscriber(i);
                Subscriber subscriber2 = new Subscriber(j);
                try {
                    int flag = 0;
                    while (flag < 3) {
                        if (broker.fetch(subscriber1).isPresent()) {
                            total.get(i).incrementAndGet();
                        } else {
                            flag |= 1;
                        }
                        if (broker.fetch(subscriber2).isPresent()) {
                            total.get(j).incrementAndGet();
                        } else {
                            flag |= 2;
                        }
                    }
                } catch (Exception ignore) {
                }
            }
        }).collect(Collectors.toList());

        threads.forEach(Thread::start);
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (Exception ignore) {
            }
        });

        IntStream.range(0, threadCount).forEach(i -> assertEquals(total.get(i).get(), 4 * messageCount));
        broker.close();
    }

    @Test
    public void memoryTest() throws Exception {

        int messageCount = 100;

        deleteDirectory();

        Broker broker = new Broker(DIRECTORY_NAME);
        broker.subscribe(SUBS1, TOP1);
        for (int i = 0; i < messageCount; ++i) {
            broker.publish(TOP1, new Message("" + i));
        }

        broker.subscribe(SUBS2, TOP1);
        for (int i = 0; i < messageCount; ++i) {
            broker.publish(TOP1, new Message("" + (i + messageCount)));
        }

        for (int i = 0; i < messageCount; ++i) {
            broker.fetch(SUBS1);
        }

        broker.close();
        broker = new Broker(DIRECTORY_NAME);

        int subs1Res = 0;
        int subs2Res = 0;

        while (broker.fetch(SUBS1).isPresent()) {
            ++subs1Res;
        }
        while (broker.fetch(SUBS2).isPresent()) {
            ++subs2Res;
        }

        assertEquals(messageCount, subs1Res);
        assertEquals(messageCount, subs2Res);

        broker.close();
        deleteDirectory();
    }

}
