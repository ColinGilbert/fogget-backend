/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backendtest;

import noob.plantsystem.backend.Backend;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 *
 * @author noob
 */
public class BackendTest {
    
    static Backend backend = new Backend();

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    public void validate() {}

    @BeforeClass
    public static void setUpClass() throws Exception {
        
        try {
            backend.init();
        } catch (MqttException e) {
            System.out.println("Caught mqqtexception initializing backend. " + e);
        }
        backend.connect();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
            backend.disconnect();
    }

    @BeforeMethod
    public void setUpMethod() throws Exception {
            backend.pushStateDataToUI();
            backend.pushEventsToUI();
            backend.pushDescriptionsToUI();
            backend.markAbsentSystems();
    }

    @AfterMethod
    public void tearDownMethod() throws Exception {
    }
}
