/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import noob.plantsystem.common.ArduinoProxy;
import java.util.HashMap;
import javafx.util.Pair;

/**
 *
 * @author noob
 * This class prevents the user from messing up the system by directly altering the container itself.
 */
public class LiveSystemPool {

    public LiveSystemPool() {
        systems = new HashMap<>();
    }
    
    public boolean add(long uid, ArduinoProxy duino) {
        if (!systems.containsKey(uid)) {
            systems.put(uid, duino);
            return true;
        }
        return false;
    }

    public boolean update(long uid, ArduinoProxy duino)
    {
        if (systems.containsKey(uid)) {
            systems.put(uid, duino);
            return true;
        }
        return false;
    }
    
    public boolean remove(long uid) {
        if (systems.containsKey(uid))
        {
            systems.remove(uid);
            return true;
        }
        return false;
    }
    
    public Pair<Boolean, ArduinoProxy> getState(long uid)
    {
        if (!systems.containsKey(uid))
        {
            return new Pair<>(false, new ArduinoProxy());
        }
        else
        {
            ArduinoProxy p = systems.get(uid);
            return new Pair<>(true, p);
        }
    }
    
    protected HashMap<Long, ArduinoProxy> systems;
}
