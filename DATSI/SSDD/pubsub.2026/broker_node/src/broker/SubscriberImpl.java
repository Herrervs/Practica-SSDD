// Clase que implementa la interfaz remota Subscriber
package broker;
import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.UUID;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import pubsub.Event;

class SubscriberImpl extends UnicastRemoteObject implements Subscriber  {
    public static final long serialVersionUID=1234567890L;
    UUID subUUID; // para facilitar depuración
    PubSubImpl ps; // para acceder a funcionalidad del servicio general
    // para notificar al subscriptor de creación y destrucción de temas
    transient SubscriberCallback scbk;
    private final LinkedList<Event> eventQueue = new LinkedList<>();
    private final List<String> subscribedTopics = new ArrayList<>();
    private boolean finished = false;

    public SubscriberImpl(PubSubImpl p, SubscriberCallback s) throws RemoteException {
        scbk=s;
        subUUID = UUID.randomUUID();
        ps=p;
    }

    private void checkFinished() throws NoSuchObjectException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
    }

    public UUID getUUID() throws RemoteException {
        checkFinished();
        return subUUID;
    }

    public int subscribe(String topic, boolean glob) throws RemoteException {
        checkFinished();
        int count = 0;
        if (!glob) {
            Topic t = ps.topics.get(topic);
            if (t != null && !subscribedTopics.contains(topic)) {
                t.addSubscriber(this);
                subscribedTopics.add(topic);
                count = 1;
            }
        } else {
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + topic);
            for (String tName : ps.topics.keySet()) {
                if (matcher.matches(Paths.get(tName)) && !subscribedTopics.contains(tName)) {
                    ps.topics.get(tName).addSubscriber(this);
                    subscribedTopics.add(tName);
                    count++;
                }
            }
        }
        return count;
    }

    public Event getEvent() throws RemoteException {
        checkFinished();
        return eventQueue.isEmpty() ? null : eventQueue.removeFirst();
    }

    public void enqueueEvent(Event ev) {
        eventQueue.add(ev);
    }

    public Collection<String> topicListBySubscriber() throws RemoteException {
        checkFinished();
        return new ArrayList<>(subscribedTopics);
    }

    public boolean unsubscribe(String topic) throws RemoteException {
        checkFinished();
        Topic t = ps.topics.get(topic);
        if (t == null || !subscribedTopics.contains(topic)) return false;
        t.removeSubscriber(this);
        subscribedTopics.remove(topic);
        return true;
    }

    public void removeTopicFromList(String topic) {
        subscribedTopics.remove(topic);
    }

    public SubscriberCallback getCallback() {
        return scbk;
    }

    public void exit() throws RemoteException {
        checkFinished();
        for (String topic : new ArrayList<>(subscribedTopics)) {
            Topic t = ps.topics.get(topic);
            if (t != null) t.removeSubscriber(this);
        }
        subscribedTopics.clear();
        ps.removeSubscriber(this);
        finished = true;
    }
}
