// Servidor que implementa la interfaz remota PubSub
package broker;
import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import pubsub.Event;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;

class PubSubImpl extends UnicastRemoteObject implements PubSub  {
    public static final long serialVersionUID=1234567890L;
    final Map<String, Topic> topics = new HashMap<>();
    private final List<SubscriberImpl> subscribers = new ArrayList<>();

    public PubSubImpl() throws RemoteException {
    }

    public int getVersion() throws RemoteException { // ya programada
        return version;
    }

    public synchronized boolean createTopic(String topic) throws RemoteException {
        if (topics.containsKey(topic)) return false;
        topics.put(topic, new Topic());
        for (SubscriberImpl sub : subscribers) {
            SubscriberCallback cb = sub.getCallback();
            if (cb != null) {
                try {
                    cb.topicAdded(topic);
                } catch (RemoteException e) {
                }
            }
        }
        return true;
    }

    public synchronized Collection<String> topicList() throws RemoteException {
        return new ArrayList<>(topics.keySet());
    }

    public synchronized boolean publish(Event ev) throws RemoteException {
        Topic t = topics.get(ev.getTopic());
        if (t == null) return false;
        t.addEvent(ev);
        for (SubscriberImpl sub : t.getSubscribers()) {
            sub.enqueueEvent(ev);
        }
        return true;
    }

    public synchronized Event consumeEvent(String topic) throws RemoteException {
        Topic t = topics.get(topic);
        if (t == null) throw new NoSuchObjectException("topic does not exist");
        return t.consumeEvent();
    }

    public synchronized Subscriber initSubscriber(SubscriberCallback c) throws RemoteException {
        SubscriberImpl sub = new SubscriberImpl(this, c);
        subscribers.add(sub);
        return sub;
    }

    public synchronized Collection<Subscriber> subscriberList() throws RemoteException {
        return new ArrayList<>(subscribers);
    }

    public synchronized Collection<Subscriber> subscriberListByTopic(String topic) throws RemoteException {
        Topic t = topics.get(topic);
        if (t == null) return null;
        return new ArrayList<>(t.getSubscribers());
    }

    public synchronized boolean deleteTopic(String topic) throws RemoteException {
        Topic t = topics.get(topic);
        if (t == null) return false;
        for (SubscriberImpl sub : new ArrayList<>(t.getSubscribers())) {
            sub.removeTopicFromList(topic);
        }
        for (SubscriberImpl sub : subscribers) {
            SubscriberCallback cb = sub.getCallback();
            if (cb != null) {
                try {
                    cb.topicRemoved(topic);
                } catch (RemoteException e) {
                }
            }
        }
        topics.remove(topic);
        return true;
    }

    void removeSubscriber(SubscriberImpl sub) {
        subscribers.remove(sub);
    }

    static public void main (String args[])  {
        if (args.length!=1) {
            System.err.println("Usage: PubSubImpl registryPortNumber");
            return;
        }
        try {
            PubSub ps = new PubSubImpl();
            Server.init(ps, args[0]);
        }
        catch (Exception e) {
            System.err.println("PubSubImpl exception: " + e.toString());
            System.exit(1);
        }
    }
}
