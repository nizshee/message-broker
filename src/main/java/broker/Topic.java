package broker;


public class Topic {

    public final int id;

    public Topic(int id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Topic && id == ((Topic) o).id;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(id);
    }
}