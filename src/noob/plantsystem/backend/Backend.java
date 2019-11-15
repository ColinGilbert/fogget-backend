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

import java.util.TreeMap;
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
import java.util.HashSet;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.util.Pair;

/**
 *
 * @author noob
 */
public class Backend implements MqttCallback {

    final protected long embeddedTimeout = 5000;
    final protected String uiCommIP = "127.0.0.1";
    final protected int uiCommPort = 6777;
    protected EventPool events = new EventPool(1000);
    protected TreeMap<Long, ArduinoProxy> systems = new TreeMap<>();
    protected TreeMap<Long, String> systemDescriptions = new TreeMap<>();
    protected HashSet<Long> liveSystems = new HashSet<>();
    protected ArduinoEventDescriptions eventDescriptions = new ArduinoEventDescriptions();

    protected long currentTime;
    //MQTT related
    protected String brokerURL = "tcp://127.0.0.1:1883";
    protected MqttAsyncClient client;
    protected boolean logging;
    protected MqttConnectOptions connectionOptions;
    protected Object waiter = new Object();

    public void init() throws MqttException {
        connectionOptions = new MqttConnectOptions();
        connectionOptions.setCleanSession(true);
        connectionOptions.setMaxInflight(10000);
        client = new MqttAsyncClient(brokerURL, MqttAsyncClient.generateClientId(), new MemoryPersistence());
        currentTime = System.currentTimeMillis();
        client.setCallback(this);
    }

    void setLogging(boolean arg) {
        logging = arg;
    }

    public void connect() {
        MqttConnector con = new MqttConnector();
        con.doConnect();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }
        subscribe(TopicStrings.stateControlRequest(), 2);
        subscribe(TopicStrings.embeddedTransientStatePush(), 0);
        // subscribe(TopicStrings.embeddedEvent(), 2);
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

    public void markAbsentSystems() {
        long now = System.currentTimeMillis();
        for (Long key : systems.keySet()) {
            ArduinoProxy p = systems.get(key);
            if ((now - p.getTimestamp()) > embeddedTimeout) {
                liveSystems.remove(key);
            }
        }
    }

    // Logic to push configuration to embedded system.
    public void pushConfig(ArduinoConfigChangeRepresentation arg) {
        if (arg.hasChanges()) { // This prevents us from wasting MQTT requests on empty items.
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
            // log("Config pushed. Topic string: " + topic + ", JSON: " + messageStr);
        }
    }

    public void pushDescriptionsToUI() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Socket socket = new Socket(uiCommIP, uiCommPort);
            // Scanner tcpIn = new Scanner(socket.getInputStream());
            PrintWriter tcpOut = new PrintWriter(socket.getOutputStream(), true);
            tcpOut.println("PUTDESCRIPTIONS");
            TreeMap<Long, String> results = new TreeMap<>();
            for (long key : systemDescriptions.keySet()) {
                if (liveSystems.contains(key)) {
                    results.put(key, systemDescriptions.get(key));
                }
            }
            String info = mapper.writeValueAsString(results);
            tcpOut.println(info);
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void pushEventsToUI() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Socket socket = new Socket(uiCommIP, uiCommPort);
            // Scanner tcpIn = new Scanner(socket.getInputStream());
            PrintWriter tcpOut = new PrintWriter(socket.getOutputStream(), true);
            tcpOut.println("PUTEVENTS");
            TreeMap<Long, ArrayDeque<EventRecord>> results = new TreeMap<>();
            for (long key : events.getRaw().keySet()) {
                if (liveSystems.contains(key)) {
                    results.put(key, events.getRaw().get(key));
                }
            }
            String info = mapper.writeValueAsString(results);
            tcpOut.println(info);
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void pushStateDataToUI() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Socket socket = new Socket(uiCommIP, uiCommPort);
            // Scanner tcpIn = new Scanner(socket.getInputStream());
            PrintWriter tcpOut = new PrintWriter(socket.getOutputStream(), true);
            tcpOut.println("PUTPROXIES");
            TreeMap<Long, ArduinoProxy> results = new TreeMap<>();
            for (long key : systems.keySet()) {
                if (liveSystems.contains(key)) {
                    results.put(key, systems.get(key));
                }
            }
            String info = mapper.writeValueAsString(results);
            tcpOut.println(info);
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }
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
        // log("Delivery complete! " + token.toString());

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws MqttException {
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
            log("Got and event! ");
            handleEmbeddedEvent(splitTopic, message);
        } else if (initialTopic.equals(TopicStrings.embeddedTransientStatePush())) { // We have just been given our periodic status update from one of our systems.
            handleEmbeddedStatePush(splitTopic, message);
        } else if (initialTopic.equals(TopicStrings.stateControlRequest())) {
            log("Got state control request");
            handleControllerRequest(message);
        } else {
            log("Unknown MQTT topic received: " + topic);
        }
    }

    protected void handleEmbeddedEvent(String[] splitTopic, MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        Integer info;
        try {
            info = objectMapper.readValue(message.toString(), Integer.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        long uid = Long.parseLong(splitTopic[1]);
        if (systems.containsKey(uid)) { // If we know the uid already, we can send to the device any its config info.
            events.add(uid, System.currentTimeMillis(), info);
            ArduinoEventDescriptions descr = new ArduinoEventDescriptions();
            log("Event \"" + info + "\" added to log.");
        } else {
            log("Received event for unknown device  " + uid);
        }
    }

    protected void handleEmbeddedStatePush(String[] splitTopic, MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        ArduinoProxy info;
        try {
            info = objectMapper.readValue(message.toString(), ArduinoProxy.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        final long uid = info.getUid();
        info.setTimestamp(System.currentTimeMillis());
        if (systems.containsKey(uid)) {
            if (!liveSystems.contains(uid)) {
                liveSystems.add(uid);
                log("Re-adding to live systems!");
            }
            systems.replace(uid, info);
        } else {
            ArduinoProxy proxy = ArduinoProxySaneDefaultsFactory.get();
            proxy.setUid(uid);
            systems.put(uid, proxy);
            subscribeToEmbeddedSystem(uid);
            ArduinoConfigChangeRepresentation representation = new ArduinoConfigChangeRepresentation();
            representation.setUid(uid);
            representation.updateConfigValues(info);
            representation.changeAll();
            pushConfig(representation);
        }
    }

    protected void handleControllerRequest(MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayList<ArduinoConfigChangeRepresentation> requestItems = new ArrayList<>();
        log("Got a request to change state!");
        //readValue(response, new TypeReference<ArrayList<ArduinoProxy>>() {} )
        try {
            requestItems = objectMapper.readValue(message.toString(), new TypeReference<ArrayList<ArduinoConfigChangeRepresentation>>() {
            });
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        // Send the control message to the relevant embedded devices
        for (ArduinoConfigChangeRepresentation req : requestItems) {
            if (req.hasChanges()) {
                pushConfig(req);
                log("Sending a control packet");
            }
        }
    }

    protected void subscribeToEmbeddedSystem(long uid) {
        String statusReportTopic = TopicStrings.embeddedTransientStatePush();
        statusReportTopic += "/";
        statusReportTopic += uid;
        subscribe(statusReportTopic, 0);
        String eventTopic = TopicStrings.embeddedEvent();
        eventTopic += "/";
        eventTopic += uid;
        subscribe(eventTopic, 2);
    }

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

}
