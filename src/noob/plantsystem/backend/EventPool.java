/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Collections;
import javafx.util.Pair;

import noob.plantsystem.common.ArduinoEventDescriptions;
import noob.plantsystem.common.EventRecord;

/**
 *
 * @author noob
 */
public class EventPool {

    public EventPool(int bufferSize) {
        capacity = bufferSize;
        events = new HashMap<>();
    }

    public synchronized void add(long uidArg, long timestampArg, int eventArg) {
        if (events.containsKey(uidArg)) {
            EventRecord rec = new EventRecord();
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

    public Pair<Boolean, ArrayDeque<EventRecord>> getEvents(long uid) {
        if (events.containsKey(uid)) {
          return new Pair<>(true, events.get(uid));
        }
        else {
            return new Pair<>(false, new ArrayDeque<>());
        }
    }
    
    protected HashMap<Long, ArrayDeque<EventRecord>> events;
    protected ArduinoEventDescriptions descriptions;
    int capacity;
}
