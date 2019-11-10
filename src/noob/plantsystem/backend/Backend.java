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
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

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

/**
 *
 * @author noob
 */
public class Backend implements MqttCallback {

    public void init(boolean clean) throws MqttException {
        connectionOptions = new MqttConnectOptions();
        connectionOptions.setCleanSession(true);
        client = new MqttAsyncClient(brokerURL, Long.toString(1));
    }

    void setLogging(boolean arg) {
        logging = arg;
    }

    // Required callbacks, implementing the MQTT library interface requirements.
    @Override
    public void connectionLost(Throwable cause) {
        // TODO: Add reconnect logic
        ex = cause;
        log("Connection lost! Cause" + ex.toString());
        connect();
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
        byte[] contents = message.toString().getBytes();
        ObjectMapper objectMapper = new ObjectMapper();
        if (topic.equals(TopicStrings.embeddedHello())) { // We've just been informed of a fresh system within our purview.
            log("Got a hello message.");
            // Add new arduino to system pool. Add its state to our systems pool.
            try {
                JsonNode rootNode = objectMapper.readTree(contents);
                JsonNode currentNode = rootNode.path(ArduinoPropertyStrings.uid());
                if (currentNode.isMissingNode()) {
                    log("Received hello with missing UID. Aborting add.");
                    return;
                }
                final long uid = currentNode.asLong();
                if (systems.containsKey(uid)) { // If we know the uid already, we can send to the device any its config info.
                    ArduinoProxy proxy = systems.get(uid);
                    pushConfig(proxy.getPersistentState());
                } else {  // We create a new representation and offer it sane defaults.
                    ArduinoProxy proxy = ArduinoProxySaneDefaultsFactory.get();
                    PersistentArduinoState state = proxy.getPersistentState();
                    state.setUID(uid);
                    proxy.setPersistentState(state);
                    systems.put(currentNode.asLong(), proxy);
                }
                String freshTopic = TopicStrings.embeddedStatusReport();
                freshTopic += "/";
                freshTopic += uid;

                subscribe(freshTopic, 1);
            } catch (IOException e) {
                log("Exception caught while mapping new Arduino into livepool. Cause: " + e.toString());
            }
        } else if (initialTopic.equals(TopicStrings.embeddedEvent())) { // We have been just informed of an event that is worth logging...
            try {
                JsonNode rootNode = objectMapper.readTree(contents);
                JsonNode currentNode = rootNode.path(ArduinoPropertyStrings.uid());
                if (currentNode.isMissingNode()) {
                    log("Received important event with missing uid. Aborting event updates.");
                    return;
                }
                final long uid = currentNode.asLong();
                if (systems.containsKey(uid)) {
                    ArduinoProxy proxy = systems.get(uid);
                    currentNode = rootNode.path("event");
                    if (currentNode.isMissingNode()) {
                        log("Received important event with missing event description.");
                        return;
                    }
                    final String event = currentNode.asText();
                    if (eventDescriptions.exists(event)) {
                        EventRecord rec = new EventRecord();
                        events.add(uid, System.currentTimeMillis(), eventDescriptions.getCode(event).getValue());
                    } else {
                        log("Important event received with invalid event number");
                    }
                }
            } catch (IOException e) {
                log("Exception caught while receiving an important event from an embedded system. Cause: " + e.toString());
            }
        } else if (initialTopic.equals(TopicStrings.embeddedStatusReport())) { // We have just been given our periodic status update from one of our systems.
            // Time to compare values in our existing pool and update when necessary.
            try {
                JsonNode rootNode = objectMapper.readTree(contents);
                JsonNode currentNode = rootNode.path(ArduinoPropertyStrings.uid());
                if (currentNode.isMissingNode()) {
                    log("Received status update without uid. Cannot update any values.");
                    return;
                }
                TransientArduinoState trans = new TransientArduinoState();
                TransientStateLastUpdated lastUpdated = trans.getLastUpdated();
                final long currentTime = System.currentTimeMillis();
                currentNode = rootNode.path(ArduinoPropertyStrings.timeOfDay());
                if (!currentNode.isMissingNode()) {
                    trans.setTimeOfDay(currentNode.asLong());
                    lastUpdated.setTimeOfDay(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.reservoirLevel());
                if (!currentNode.isMissingNode()) {
                    trans.setReservoirLevel(currentNode.floatValue());
                    lastUpdated.setReservoirLevel(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.nutrientSolutionLevel());
                if (!currentNode.isMissingNode()) {
                    trans.setNutrientSolutionLevel(currentNode.floatValue());
                    lastUpdated.setNutrientSolutionLevel(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.lights());
                if (!currentNode.isMissingNode()) {
                    trans.setLights(currentNode.asBoolean());
                    lastUpdated.setLights(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.powered());
                if (!currentNode.isMissingNode()) {
                    trans.setPowered(currentNode.asBoolean());
                    lastUpdated.setPowered(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.doorsLocked());
                if (!currentNode.isMissingNode()) {
                    trans.setDoorsLocked(currentNode.asBoolean());
                    lastUpdated.setLocked(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.doorsOpen());
                if (!currentNode.isMissingNode()) {
                    trans.setDoorsOpen(currentNode.asBoolean());
                    lastUpdated.setDoorsOpen(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.timeLeftUnlocked());
                if (!currentNode.isMissingNode()) {
                    trans.setTimeLeftUnlocked(currentNode.asLong());
                    lastUpdated.setTimeLeftUnlocked(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.upperChamberHumidity());
                if (!currentNode.isMissingNode()) {
                    trans.setUpperChamberHumidity(currentNode.floatValue());
                    lastUpdated.setUpperChamberHumidity(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.upperChamberTemperature());
                if (!currentNode.isMissingNode()) {
                    trans.setUpperChamberTemperature(currentNode.floatValue());
                    lastUpdated.setUpperChamberTemperature(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.lowerChamberTemperature());
                if (!currentNode.isMissingNode()) {
                    trans.setLowerChamberTemperature(currentNode.floatValue());
                    lastUpdated.setLowerChamberTemperature(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.CO2PPM());
                if (!currentNode.isMissingNode()) {
                    trans.setCO2PPM(currentNode.asInt());
                    lastUpdated.setCO2PPM(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.dehumidifying());
                if (!currentNode.isMissingNode()) {
                    trans.setDehumidifying(currentNode.asBoolean());
                    lastUpdated.setDehumidifying(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.cooling());
                if (!currentNode.isMissingNode()) {
                    trans.setCooling(currentNode.asBoolean());
                    lastUpdated.setCooling(currentTime);
                }
                currentNode = rootNode.path(ArduinoPropertyStrings.injectingCO2());
                if (!currentNode.isMissingNode()) {
                    trans.setInjectingCO2(currentNode.asBoolean());
                    lastUpdated.setInjectingCO2(currentTime);
                }
            } catch (IOException e) {
                log("Exception caught while receiving a routine update from an embedded system. Cause: " + e.toString());
            }
        } else {
            log("Unknown MQTT topic received:" + topic);
        }
    }

    // End of MQTT interface callbacks.
    public void connect() {
        MqttConnector con = new MqttConnector();
        con.doConnect();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("Wait interrupted: " + e);
        }
        subscribe("+", 1);
        subscribe(TopicStrings.embeddedHello(), 1);
    }

    public void disconnect() {
        Disconnector disc = new Disconnector();
        disc.doDisconnect();
    }

    public void subscribe(String topic, int qos) {
        Subscriber sub = new Subscriber();
        sub.doSubscribe(topic, qos);
    }

    public void pushConfig(PersistentArduinoState arg) {
        // Logic to push to embedded system.
        ObjectMapper objectMapper = new ObjectMapper();
        String messageStr;
        try {
            messageStr = objectMapper.writeValueAsString(arg);
        } catch (JsonProcessingException e) {
            log("Problem writing JSON to string in pushConfig. Reason: " + e.getMessage());
            return;
        }
        String topic = TopicStrings.embeddedStatusReport();
        topic += "/";
        topic += Long.toString(arg.getUID());
        Publisher pub = new Publisher();
        pub.doPublish(topic, 1, messageStr.getBytes());
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
                    ex = exception;
                    // state = ERROR;
                    log("Connect failed" + exception);
                    carryOn();
                }

                public void carryOn() {
                    synchronized (waiter) {
                        // state = ERROR;
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
                    ex = exception;
                    log("Publish failed" + exception);
                    // state = ERROR;
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
                // state = ERROR;
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
                    ex = exception;
                    // state = ERROR;
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
                // state = ERROR;
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
                    ex = exception;
                    log("Disconnect failed" + exception);
                    // state = ERROR;
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
                // state = ERROR;
                doNext = true;
                ex = e;
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
    protected ArduinoEventDescriptions eventDescriptions = new ArduinoEventDescriptions();
    protected HashMap<Long, String> systemDescriptions = new HashMap<>();

    //MQTT related
    protected String brokerURL = "tcp://127.0.0.1:1883";
    protected MqttAsyncClient client;
    protected boolean logging;
    protected MqttConnectOptions connectionOptions;

    // protected boolean clean;
    protected Throwable ex = null;
    protected Object waiter = new Object();
    protected boolean doNext = false;

}
