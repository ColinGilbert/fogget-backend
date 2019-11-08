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
import noob.plantsystem.common.SerializedArduinoState;
/**
 *
 * @author noob
 */
public class ArduinoPropertySerializer {

    public boolean save(ArduinoProxy arg) {
        boolean success = true;
        File tentativePath = new File(String.valueOf(arg.getUID()) + ".TEMP");
        try {
            FileOutputStream fileOut = new FileOutputStream(tentativePath);
            ObjectOutputStream oos = new ObjectOutputStream(fileOut);
            oos.writeObject(arg.getSerializedState());
            oos.flush();
            fileOut.flush();
            success = tentativePath.renameTo(new File(String.valueOf(arg.getUID())));
        } catch (FileNotFoundException e) { success = false; }
        catch (IOException e) { success = false; }
        finally { }
        
        return success;
    }

        
    public Pair<Boolean, ArduinoProxy> load(ArduinoProxy arg) {
        Boolean success = true;
        ArduinoProxy results = arg;
        File tentativePath = new File(String.valueOf(arg.getUID()));
        
        try {
            FileInputStream fileIn = new FileInputStream(tentativePath);
            ObjectInputStream ois = new ObjectInputStream(fileIn);
            results.setSerializedState((SerializedArduinoState)ois.readObject());
        } catch (FileNotFoundException e) { success = false; }
        catch (IOException | ClassNotFoundException e) { success = false; }
        finally { }
        
        return new Pair<>(success, results);
    }

}
