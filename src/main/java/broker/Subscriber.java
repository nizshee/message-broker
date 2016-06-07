package broker;


public class Subscriber {

    public final int id;

    public Subscriber(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Subscriber && id == ((Subscriber) o).id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }


}
