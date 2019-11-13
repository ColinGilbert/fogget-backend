/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javafx.util.Pair;


import noob.plantsystem.common.ArduinoProxy;
import noob.plantsystem.common.PersistentArduinoState;
/**
 *
 * @author noob
 */
public class ArduinoFilesystemSaver {

    public boolean save(ArduinoProxy arg) {
        boolean success = true;
        PersistentArduinoState state = arg.extractPersistentState();
        File tentativePath = new File(String.valueOf(state.getUid()) + ".TEMP");
        try {
            FileOutputStream fileOut = new FileOutputStream(tentativePath);
            ObjectOutputStream oos = new ObjectOutputStream(fileOut);
            oos.writeObject(arg.extractPersistentState());
            oos.flush();
            fileOut.flush();
            success = tentativePath.renameTo(new File(String.valueOf(state.getUid())));
        } catch (FileNotFoundException e) { success = false; }
        catch (IOException e) { success = false; }
        finally { }
        
        return success;
    }

        
    public Pair<Boolean, ArduinoProxy> load(long uid) {
        Boolean success = true;
        ArduinoProxy results = new ArduinoProxy();
        File tentativePath = new File(String.valueOf(uid));
        try {
            FileInputStream fileIn = new FileInputStream(tentativePath);
            ObjectInputStream ois = new ObjectInputStream(fileIn);
            results.updatePersistentState((PersistentArduinoState)ois.readObject());
        } catch (FileNotFoundException e) { success = false; }
        catch (IOException | ClassNotFoundException e) { success = false; }
        finally { }
        
        return new Pair<>(success, results);
    }

}
