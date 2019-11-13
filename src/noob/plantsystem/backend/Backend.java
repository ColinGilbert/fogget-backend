/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.fasterxml.jackson.core.TokenStreamFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.util.Pair;

/**
 *
 * @author noob
 */
public class Backend implements MqttCallback {

    public void init() throws MqttException {
        connectionOptions = new MqttConnectOptions();
        connectionOptions.setCleanSession(false);
        client = new MqttAsyncClient(brokerURL, Long.toString(1), new MemoryPersistence());
        client.setCallback(this);
    }

    void setLogging(boolean arg) {
        logging = arg;
    }

    // Here are the classes implemended by me, Colin. :)
    public void connect() {
        MqttConnector con = new MqttConnector();
        con.doConnect();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }
        // subscribe(TopicStrings.embeddedHello(), 2);
    }

    public void disconnect() {
        Disconnector disc = new Disconnector();
        disc.doDisconnect();
    }

    public void subscribe(String topic, int qos) {
        Subscriber sub = new Subscriber();
        sub.doSubscribe(topic, qos);
    }

    public void publish(String topic, int qos, byte[] payload) {
        Publisher pub = new Publisher();
        pub.doPublish(topic, qos, payload);
    }

    // Logic to push configuration to embedded system.
    public void pushConfig(PersistentArduinoState arg) {
        log("Pushing configuration");
        ObjectMapper objectMapper = new ObjectMapper();
        String messageStr;
        try {
            messageStr = objectMapper.writeValueAsString(arg);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        String topic = TopicStrings.configPushToEmbedded();
        topic += "/";
        topic += Long.toString(arg.getUid());
        try {
            client.publish(topic, messageStr.getBytes(), 2, true);
        } catch (MqttException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }
        log("Config pushed. Topic string: " + topic + ", JSON: " + messageStr);
    }

    // Required callbacks, implementing the MQTT library interface requirements.
    @Override
    public void connectionLost(Throwable cause) {
        log("Connection lost! Cause" + cause.toString());
        // connect();
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Logic implementing what we do when we know stuff got delivered.
        log("Delivery complete! " + token.toString());

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws MqttException {
        log("Got a message.");
        // Here is where the real fun begins. Most of what we do is in response to messages arriving into our system.
        String splitTopic[] = topic.split("/");
        if (splitTopic.length == 0) {
            log("Received a message without any topic. Cannot do anything with it.");
            return;
        }
        String initialTopic = splitTopic[0];
        if (initialTopic.equals(TopicStrings.embeddedEvent())) { // We have been just informed of an event that is worth logging...
            if (splitTopic.length < 2) {
                log("No uid in topic string for embedded event message.");
                return;
            }
            handleEmbeddedEvent(splitTopic, message);
        } else if (initialTopic.equals(TopicStrings.embeddedTransientStatePush())) { // We have just been given our periodic status update from one of our systems.
            // Time to compare values in our existing pool and update when necessary.
            if (splitTopic.length < 2) {
                log("No uid in topic string for embedded status report.");
                return;
            }
            handleEmbeddedTransientStatePush(splitTopic, message);
        } else if (initialTopic.equals(TopicStrings.systemsViewRequest())) {
            handleSystemsViewRequest(message);
        } else if (initialTopic.equals(TopicStrings.eventsViewRequest())) {
            handleEventsViewRequest(message);
        } else if (initialTopic.equals(TopicStrings.stateControlRequest())) {
            handleStateControlRequest(message);
        } else {
            log("Unknown MQTT topic received: " + topic);
        }
    }

    /*
    protected void handleEmbeddedHello(MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        HelloMessageRepresentation hello;
        try {
            hello = objectMapper.readValue(message.toString(), HelloMessageRepresentation.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        final long uid = hello.getUid();


}
     */
    protected void handleEmbeddedEvent(String[] splitTopic, MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        ArduinoEvent info;
        try {
            info = objectMapper.readValue(message.toString(), ArduinoEvent.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        final long uid = info.getUid();
        if (uid != Long.parseLong(splitTopic[1])) {
            log("Event UID and topic mismatch in event received. UID = " + uid + ". Received data: " + splitTopic[1]);
            return;
        }
        if (systems.containsKey(uid)) { // If we know the uid already, we can send to the device any its config info.
            events.add(uid, info.getTimestamp(), info.getEvent());
            ArduinoEventDescriptions descr = new ArduinoEventDescriptions();
            log("Event \"" + descr.getDescription(info.getEvent()) + "\"added to log.");
        } else {
            log("Received event for unknown device.");
        }
    }

    protected void handleEmbeddedTransientStatePush(String[] splitTopic, MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        EmbeddedStatusReport info;
        try {
            info = objectMapper.readValue(message.toString(), EmbeddedStatusReport.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        if (info.getUid() != Long.parseLong(splitTopic[1])) {
            log("Transient state UID and topic mismatch. UID = " + info.getUid() + ".Received data: " + splitTopic[1]);
            return;
        }
        final long uid = info.getUid();
        if (systems.containsKey(info.getUid())) {
            ArduinoProxy proxy = systems.get(info.getUid());
            proxy.updateTransientState(info.makeFromTransientState());
            systems.replace(info.getUid(), proxy);
            log("Status report for " + info.getUid() + " received.");
        } else {
            ArduinoProxy proxy = ArduinoProxySaneDefaultsFactory.get();
            proxy.setTargetUpperChamberHumidity(70.0f);
            PersistentArduinoState state = proxy.extractPersistentState();
            state.setUid(uid);
            proxy.updatePersistentState(state);
            systems.put(uid, proxy);
            subscribeToEmbeddedSystem(uid);
            pushConfig(proxy.extractPersistentState());
        }
    }


    protected void handleSystemsViewRequest(MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<ArduinoProxy> responseData = new ArrayList<>();
        String responseStr = "";
/*
        try {
            responseStr = objectMapper.writeValueAsString(responseData);

        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        */
        // Publish response over MQTT

        publish(TopicStrings.systemsViewResponse(), 2, responseStr.getBytes());
    }

    protected void handleEventsViewRequest(MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        EventsViewRequestRepresentation request;
        try {
            request = objectMapper.readValue(message.toString(), EventsViewRequestRepresentation.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;

        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        /*
        EventsViewResponseRepresentation responseData = new EventsViewResponseRepresentation();
        Pair<Boolean, ArrayDeque<EventRecord>> records = events.getEvents(request.getUid());
        responseData.setEvents(records.getValue());
        responseData.setUid(request.getUid());
        String responseStr = "";
        try {
            responseStr = objectMapper.writeValueAsString(responseData);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        publish(TopicStrings.eventsViewResponse(), 2, responseStr.getBytes());
*/
    }

    protected void handleStateControlRequest(MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        PersistentArduinoState request;
        try {
            request = objectMapper.readValue(message.toString(), PersistentArduinoState.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        // Send the control message to the relevant embedded devices

    }

    protected void subscribeToEmbeddedSystem(long uid) {
        String statusReportTopic = TopicStrings.embeddedTransientStatePush();
        statusReportTopic += "/";
        statusReportTopic += uid;
        subscribe(statusReportTopic, 2);
        String eventTopic = TopicStrings.embeddedEvent();
        eventTopic += "/";
        eventTopic += uid;
        subscribe(eventTopic, 2);
    }

// End of MQTT interface callbacks.
    // The following are async helper classes from https://github.com/eclipse/paho.mqtt.java
    public class MqttConnector {

        public void doConnect() {
            // Connect to the server. Get a token and setup an asynchronous listener on the token which will be notified once the connect completes.
            log("Connecting to " + brokerURL + " with client ID " + client.getClientId());
            IMqttActionListener conListener;
            conListener = new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    // state = CONNECTED;
                    log("Connected");
                    carryOn();
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    // state = ERROR;
                    log("Connect failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        // state = ERROR;
                        waiter.notifyAll();
                    }
                }
            };

            try {
                // Connect using a non-blocking connect.
                client.connect(connectionOptions, "Connect sample context", conListener);
            } catch (MqttException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);

                // If though it is a non-blocking connect an exception can be thrown if validation of params fails or other checks such as already connected fail.
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
            IMqttActionListener pubListener;
            pubListener = new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    log("Publish Completed");
                    // state = PUBLISHED;
                    carryOn();
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log("Publish failed" + exception);
                    // state = ERROR;
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        waiter.notifyAll();
                    }
                }
            };

            try {
                // Publish the message
                client.publish(topicName, message, "Pub sample context", pubListener);
            } catch (MqttException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);

                // state = ERROR;
            }
        }
    }

// Subscribe in a non-blocking way and then sit back and wait to be notified that the action has completed.
    public class Subscriber {

        public void doSubscribe(String topicName, int qos) {
            // Make a subscription
            // Get a token and setup an asynchronous listener on the token which will be notified once the subscription is in place.
            log("Subscribing to topic \"" + topicName + "\" qos " + qos);

            IMqttActionListener subListener;
            subListener = new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    // state = SUBSCRIBED;
                    log("Subscribe Completed");
                    carryOn();
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    // state = ERROR;
                    log("Subscribe failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        waiter.notifyAll();
                    }
                }
            };

            try {
                client.subscribe(topicName, qos, "Subscribe sample context", subListener);
            } catch (MqttException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);

            }
        }
    }

// Disconnect in a non-blocking way and then sit back and wait to be notified that the action has completed.
    public class Disconnector {

        public void doDisconnect() {
            // Disconnect the client
            log("Disconnecting");

            IMqttActionListener discListener;
            discListener = new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    log("Disconnect Completed");
                    // state = DISCONNECTED;
                    carryOn();
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log("Disconnect failed" + exception);
                    // state = ERROR;
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        waiter.notifyAll();
                    }
                }
            };

            try {
                client.disconnect("Disconnect", discListener);
            } catch (MqttException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);

            }
        }
    }

    protected void log(String arg) {
        if (logging) {
            System.out.println(arg);
        }
    }

    protected EventPool events = new EventPool(1000);
    protected HashMap<Long, ArduinoProxy> systems = new HashMap<>();
    protected HashMap<Long, Long> lastUpdated = new HashMap<>();
    protected HashMap<Long, String> systemDescriptions = new HashMap<>();
    protected ArduinoEventDescriptions eventDescriptions = new ArduinoEventDescriptions();

    //MQTT related
    protected String brokerURL = "tcp://127.0.0.1:1883";
    protected MqttAsyncClient client;
    protected boolean logging;
    protected MqttConnectOptions connectionOptions;

    protected Object waiter = new Object();
}
