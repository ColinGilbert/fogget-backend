/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;


import java.util.ArrayDeque;
import java.util.TreeMap;
import javafx.util.Pair;
import noob.plantsystem.common.EventRecordMemento;

/**
 *
 * @author noob
 */
public class EventPool {

    protected TreeMap<Long, ArrayDeque<EventRecordMemento>> events;
    protected int capacity;
    
    public EventPool(int bufferSize) {
        capacity = bufferSize;
        events = new TreeMap<>();
    }

    public synchronized void add(long uidArg, long timestampArg, int eventArg) {
        if (events.containsKey(uidArg)) {
            EventRecordMemento rec = new EventRecordMemento();
            rec.setTimestamp(timestampArg);
            rec.setEvent(eventArg);
            if (events.get(uidArg).size() < capacity) {
                events.get(uidArg).addFirst(rec);
            } else {
                events.get(uidArg).removeLast();
                events.get(uidArg).addFirst(rec);
            }
        } else {
            // Add uid + preassigned array to events, then call itself again.
            events.put(uidArg, new ArrayDeque<>(capacity));
            add(uidArg, timestampArg, eventArg);
        }
    }

    public Pair<Boolean, ArrayDeque<EventRecordMemento>> getEvents(long uid) {
        if (events.containsKey(uid)) {
            return new Pair<>(true, events.get(uid));
        } else {
            return new Pair<>(false, new ArrayDeque<>());
        }
    }

    public TreeMap<Long, ArrayDeque<EventRecordMemento>> getRaw() {
        return events;
    }
}
