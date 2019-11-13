/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 *
 * @author noob
 */
public class MainExecutable {
       /**
     * @param args the command line arguments
     */

    public static void main(String[] args) {
        // TODO code application logic here
        Backend app = new Backend();
        app.setLogging(true);
        System.out.println("Starting backend!");
        try {
            app.init();
        } catch (MqttException e) {
            System.out.println("Caught mqqtexception initializing backend. " + e);
        }
        app.connect();
    }
}
