/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import java.util.ArrayList;
import java.util.HashMap;

import noob.plantsystem.common.EventDescription;
import noob.plantsystem.common.EventsResponseIterator;
import noob.plantsystem.common.EventsResponseIteratorBuilder;

/**
 *
 * @author noob
 */
public class EventPool {

    public EventPool(int bufferSize) {
        capacity = bufferSize;
        currentEvents = new HashMap<>();
        events = new HashMap<>();
    }

    public void clear() {
        currentEvents.entrySet().forEach((entry) -> {
            currentEvents.replace(entry.getKey(), 0);
        });
        events.clear();
    }

    public synchronized boolean add(long uidArg, long timestampArg, int eventArg) {
        if (events.containsKey(uidArg)) {
            if (currentEvents.containsKey(uidArg)) {
                int current = currentEvents.get(uidArg);
                if (current < capacity - 1) {
                    EventRecord rec = new EventRecord();
                    rec.timestamp = timestampArg;
                    rec.eventType = eventArg;
                    current++;
                    currentEvents.replace(uidArg, current);
                    return true;
                }
            }
            else { // Add uid to currentEvents, then call itself again.
                currentEvents.put(uidArg, 0);
                return this.add(uidArg, timestampArg, eventArg);
            }
        }
        else { // Add uid + preassigned array to events, then call itself again. It will then call itself.
            events.put(uidArg, new ArrayList<>(capacity));
            return this.add(uidArg, timestampArg, eventArg);
        }
        return false;
    }

    EventsResponseIterator getIterator() {
        EventsResponseIteratorBuilder builder = new EventsResponseIteratorBuilder();
        events.entrySet().forEach((entry) -> {
           if (currentEvents.containsKey(entry.getKey())) {
               builder.addArduino(entry.getKey());
               int max = currentEvents.get(entry.getKey());
               for (int i = 0; i < max; i++) {
                   EventRecord rec = entry.getValue().get(i);
                    builder.addEvent(rec.eventType, rec.timestamp);               }
           }
        });
        return builder.getBuiltItem();
    }

    protected HashMap<Long, ArrayList<EventRecord>> events;
    protected HashMap<Long, Integer> currentEvents;
    protected EventDescription descriptions;
    int capacity;
}
