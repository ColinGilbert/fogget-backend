/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.sql.Timestamp;

import java.util.HashMap;
import java.util.ArrayList;

import noob.plantsystem.common.*;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

/**
 *
 * @author noob
 */
public class Backend implements UserCommunicationInterface, MqttCallback {

    // Getters
    public List<Long> getActiveArduinoSystems() {
        ArrayList<Long> results = new ArrayList<>();

        return results;
    }

    public List<ArduinoProxy> getArduinoState(List<Integer> args) {
        ArrayList<ArduinoProxy> results = new ArrayList<>();

        return results;
    }

    public EventsResponseIterator getArduinoHistory(int numEvents, List<Long> uids, List<Integer> eventTypes) {
        EventsResponseIteratorBuilder builder = new EventsResponseIteratorBuilder();

        return builder.getBuiltItem();
    }

    public Map<Long, String> getArduinoDescriptions(List<Long> uids) {
        HashMap<Long, String> results = new HashMap<>();

        return results;
    }

    public Map<Integer, String> getEventDescriptions() {
        HashMap<Integer, String> results = new HashMap<>();

        return results;
    }

    public long getTime() {
        return System.currentTimeMillis();
    }

    // Setters
    public Map<Long, Boolean> setArduinoDescription(Map<Long, String> values) {
        HashMap<Long, Boolean> results = new HashMap<>();

        return results;
    }

    public Map<Long, Boolean> setMistingInterval(Map<Long, Long> values) {
        HashMap<Long, Boolean> results = new HashMap<>();

        return results;
    }

    public Map<Long, Long> setNutrientsPPM(Map<Long, Long> values) {
        HashMap<Long, Long> results = new HashMap<>();

        return results;
    }

    public Map<Long, Double> setNutrientsSolutionRatio(Map<Long, Double> values) {
        HashMap<Long, Double> results = new HashMap<>();

        return results;
    }

    public Map<Long, Boolean> setDailyLightingSchedule(List<Long> uids, long onTimeMillis, long offTimeMillis) {
        HashMap<Long, Boolean> results = new HashMap<>();

        return results;
    }

    public Map<Long, Boolean> unlock(List<Long> uids, long millis) {
        HashMap<Long, Boolean> results = new HashMap<>();

        return results;
    }

    public Map<Long, Boolean> lock(List<Long> uids) {
        HashMap<Long, Boolean> results = new HashMap<>();

        return results;
    }

    public Map<Long, Float> setTargetUpperChamberHumidity(List<Long> uids, float percentage) {
        HashMap<Long, Float> results = new HashMap<Long, Float>();

        return results;
    }

    public Map<Long, Float> setTargetUpperChamberTemperature(List<Long> uids, float percentage) {
        HashMap<Long, Float> results = new HashMap<Long, Float>();

        return results;
    }

    public Map<Long, Float> setTargetLowerChamberTemperature(List<Long> uids, float percentage) {
        HashMap<Long, Float> results = new HashMap<>();

        return results;
    }

    public Map<Long, Float> setCurrentLowerChamberTemperature(List<Long> uids, float percentage) {
        HashMap<Long, Float> results = new HashMap<>();

        return results;
    }

    protected void log(String arg) {
        if (logging) {
            System.out.println(arg);
        }
    }

    // Required callbacks, implementing the MQTT library interface requirements.
    public void connectionLost(Throwable cause) {

    }

    public void deliveryComplete(IMqttDeliveryToken token) {

    }

    public void messageArrived(String topic, MqttMessage message) throws MqttException {

    }

    public void publish(String topicName, int qos, byte[] payload) throws Throwable {
        // Use a state machine to decide which step to do next. State change occurs 
        // when a notification is received that an MQTT action has completed
        while (state != FINISH) {
            switch (state) {
                case BEGIN:
                    // Connect using a non-blocking connect
                    MqttConnector con = new MqttConnector();
                    con.doConnect();
                    break;
                case CONNECTED:
                    // Publish using a non-blocking publisher
                    Publisher pub = new Publisher();
                    pub.doPublish(topicName, qos, payload);
                    break;
                case PUBLISHED:
                    state = DISCONNECT;
                    doNext = true;
                    break;
                case DISCONNECT:
                    Disconnector disc = new Disconnector();
                    disc.doDisconnect();
                    break;
                case ERROR:
                    throw ex;
                case DISCONNECTED:
                    state = FINISH;
                    doNext = true;
                    break;
            }

//    		if (state != FINISH) {
            // Wait until notified about a state change and then perform next action
            waitForStateChange(10000);
//    		}
        }
    }

    /**
     * Wait for a maximum amount of time for a state change event to occur
     *
     * @param maxTTW maximum time to wait in milliseconds
     * @throws MqttException
     */
    private void waitForStateChange(int maxTTW) throws MqttException {
        synchronized (waiter) {
            if (!doNext) {
                try {
                    waiter.wait(maxTTW);
                } catch (InterruptedException e) {
                    log("timed out");
                    e.printStackTrace();
                }

                if (ex != null) {
                    throw (MqttException) ex;
                }
            }
            doNext = false;
        }
    }

    /**
     * Subscribe to a topic on an MQTT server Once subscribed this method waits
     * for the messages to arrive from the server that match the subscription.
     * It continues listening for messages until the enter key is pressed.
     *
     * @param topicName to subscribe to (can be wild carded)
     * @param qos the maximum quality of service to receive messages at for this
     * subscription
     * @throws MqttException
     */
    public void subscribe(String topicName, int qos) throws Throwable {
        // Use a state machine to decide which step to do next. State change occurs 
        // when a notification is received that an MQTT action has completed
        while (state != FINISH) {
            switch (state) {
                case BEGIN:
                    // Connect using a non-blocking connect
                    MqttConnector con = new MqttConnector();
                    con.doConnect();
                    break;
                case CONNECTED:
                    // Subscribe using a non-blocking subscribe
                    Subscriber sub = new Subscriber();
                    sub.doSubscribe(topicName, qos);
                    break;
                case SUBSCRIBED:
                 // Block until Enter is pressed allowing messages to arrive
                 //   log("Press <Enter> to exit");
                 //   try {
                 //       System.in.read();
                 //   } catch (IOException e) {
                 // If we can't read we'll just exit
                 //   }
                    state = DISCONNECT;
                    doNext = true;
                    break;
                case DISCONNECT:
                    Disconnector disc = new Disconnector();
                    disc.doDisconnect();
                    break;
                case ERROR:
                    throw ex;
                case DISCONNECTED:
                    state = FINISH;
                    doNext = true;
                    break;
            }

//    		if (state != FINISH && state != DISCONNECT) {
            waitForStateChange(10000);
        }
//    	}    	
    }

    // The following are async helper classes from https://github.com/eclipse/paho.mqtt.java
    public class MqttConnector {

        public MqttConnector() {
        }

        public void doConnect() {
            // Connect to the server. Get a token and setup an asynchronous listener on the token which will be notified once the connect completes.
            log("Connecting to " + brokerURL + " with client ID " + client.getClientId());

            IMqttActionListener conListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    state = CONNECTED;

                    log("Connected");
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    log("connect failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        state = ERROR;
                        doNext = true;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                // Connect using a non-blocking connect.
                client.connect(connectionOptions, "Connect sample context", conListener);
            } catch (MqttException e) {
                // If though it is a non-blocking connect an exception can be thrown if validation of params fails or other checks such as already connected fail.
                doNext = true;
                ex = e;
            }
        }
    }

    // Publish in a non-blocking way and then sit back and wait to be notified that the action has completed.
    public class Publisher {

        public void doPublish(String topicName, int qos, byte[] payload) {
            // Send / publish a message to the server.
            // Get a token and setup an asynchronous listener on the token which will be notified once the message has been delivered.
            MqttMessage message = new MqttMessage(payload);
            message.setQos(qos);

            String time = new Timestamp(System.currentTimeMillis()).toString();
            log("Publishing at: " + time + " to topic \"" + topicName + "\" qos " + qos);

            // Setup a listener object to be notified when the publish completes.
            //
            IMqttActionListener pubListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log("Publish Completed");
                    state = PUBLISHED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    log("Publish failed" + exception);
                    state = ERROR;
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {

                        doNext = true;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                // Publish the message
                client.publish(topicName, message, "Pub sample context", pubListener);
            } catch (MqttException e) {
                state = ERROR;
                doNext = true;
                ex = e;
            }
        }
    }

    // Subscribe in a non-blocking way and then sit back and wait to be notified that the action has completed.
    public class Subscriber {

        public void doSubscribe(String topicName, int qos) {
            // Make a subscription
            // Get a token and setup an asynchronous listener on the token which will be notified once the subscription is in place.
            log("Subscribing to topic \"" + topicName + "\" qos " + qos);

            IMqttActionListener subListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    state = SUBSCRIBED;
                    log("Subscribe Completed");
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    state = ERROR;
                    log("Subscribe failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        doNext = true;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                client.subscribe(topicName, qos, "Subscribe sample context", subListener);
            } catch (MqttException e) {
                state = ERROR;
                doNext = true;
                ex = e;
            }
        }
    }

    // Disconnect in a non-blocking way and then sit back and wait to be notified that the action has completed.
    public class Disconnector {

        public void doDisconnect() {
            // Disconnect the client
            log("Disconnecting");

            IMqttActionListener discListener = new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    log("Disconnect Completed");
                    state = DISCONNECTED;
                    carryOn();
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    ex = exception;
                    log("Disconnect failed" + exception);
                    state = ERROR;
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {

                        doNext = true;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                client.disconnect("Disconnect", discListener);
            } catch (MqttException e) {
                state = ERROR;
                doNext = true;
                ex = e;
            }
        }
    }

    protected LiveSystemPool systemPool;
    protected EventPool eventPool;

    //MQTT related
    protected String brokerURL;
    protected MqttAsyncClient client;
    protected boolean logging;
    protected MqttConnectOptions connectionOptions;
    protected boolean clean;
    protected Throwable ex = null;
    protected Object waiter = new Object();
    protected boolean doNext = false;

    protected int state = BEGIN;

    static final int BEGIN = 0;
    static final int CONNECTED = 1;
    static final int PUBLISHED = 2;
    static final int SUBSCRIBED = 3;
    static final int DISCONNECTED = 4;
    static final int FINISH = 5;
    static final int ERROR = 6;
    static final int DISCONNECT = 7;

}
