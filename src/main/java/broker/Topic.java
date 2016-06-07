package broker;


public class Topic {

    public final String name;

    public Topic(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Topic && name.equals(((Topic) o).name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}