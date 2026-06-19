// clase Topic
package broker;
import java.util.LinkedList;
import java.util.List;
import pubsub.Event;
import pubsub.Subscriber;

class Topic {
    private final LinkedList<Event> eventQueue = new LinkedList<>();
    private final List<SubscriberImpl> subscribers = new LinkedList<>();

    public Topic() {
    }

    public void addEvent(Event ev) {
        eventQueue.add(ev);
    }

    public Event consumeEvent() {
        return eventQueue.isEmpty() ? null : eventQueue.removeFirst();
    }

    public void addSubscriber(SubscriberImpl sub) {
        subscribers.add(sub);
    }

    public boolean removeSubscriber(SubscriberImpl sub) {
        return subscribers.remove(sub);
    }

    public List<SubscriberImpl> getSubscribers() {
        return subscribers;
    }
}
