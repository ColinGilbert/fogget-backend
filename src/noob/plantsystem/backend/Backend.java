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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author noob
 */
public class Backend implements MqttCallback {

    protected EventPool events = new EventPool(CommonValues.eventPoolQueueSize);
    protected TreeMap<Long, ArduinoProxy> systems = new TreeMap<>();
    protected TreeMap<Long, String> systemDescriptions = new TreeMap<>();
    protected HashSet<Long> liveSystems = new HashSet<>();
    protected ArduinoEventDescriptions eventDescriptions = new ArduinoEventDescriptions();
    final Object proxiesLock = new Object();
    final Object descriptionsLock = new Object();
    final Object eventsLock = new Object();
    final String stateSaveFileName = "SYSTEMS.SAVE";
    final String descriptionsSaveFileName = "DESCRIPTIONS.SAVE";
    protected long currentTime;
    protected final int embeddedTimeout = 10000;
    //MQTT related
    protected String brokerURL = CommonValues.mqttBrokerURL;
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
        subscribe(TopicStrings.stateControlRequest(), 1);
        subscribe(TopicStrings.descriptionsUpdateRequest(), 1);
        subscribe(TopicStrings.embeddedStatePush(), 0);
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
            if ((now - p.getTransientState().getTimestamp()) > embeddedTimeout) {
                liveSystems.remove(key);
            }
        }
    }

    public boolean saveSystems() {
        synchronized (proxiesLock) {
            boolean success = true;
            File tentativePath = new File(stateSaveFileName + ".TEMP");
            try {
                FileOutputStream fileOut = new FileOutputStream(tentativePath);
                ObjectMapper objMapper = new ObjectMapper();
                objMapper.writeValue(fileOut, systems);
                fileOut.flush();
                success = tentativePath.renameTo(new File(stateSaveFileName));
                fileOut.close();
            } catch (FileNotFoundException ex) {
                success = false;
            } catch (IOException ex) {
                success = false;
            } finally {
            }

            return success;
        }
    }

    public boolean saveDescriptions() {
        synchronized (descriptionsLock) {
            boolean success = true;
            File tentativePath = new File(descriptionsSaveFileName);
            try {
                FileOutputStream fileOut = new FileOutputStream(tentativePath);
                ObjectMapper objMapper = new ObjectMapper();
                objMapper.writeValue(fileOut, systemDescriptions);
                fileOut.flush();
                success = tentativePath.renameTo(new File(descriptionsSaveFileName));
                fileOut.close();
            } catch (FileNotFoundException ex) {
                success = false;
            } catch (IOException ex) {
                success = false;
            } finally {
            }
            return success;
        }
    }

    public boolean loadSystems() {
        synchronized (proxiesLock) {
            boolean success = true;
            String tentativePath = stateSaveFileName + ".TEMP";
            try {
                FileInputStream fileIn = new FileInputStream(tentativePath);
                ObjectMapper objMapper = new ObjectMapper();
                String contents = new String(Files.readAllBytes(Paths.get(tentativePath)));
                systems = objMapper.readValue(contents, new TypeReference<TreeMap<Long, ArduinoProxy>>() {
                });
                fileIn.close();
            } catch (FileNotFoundException e) {
                success = false;
            } catch (IOException ex) {
                success = false;
            } finally {
            }
            return success;
        }
    }

    public boolean loadDescriptions() {
        synchronized (descriptionsLock) {
            boolean success = true;
            String tentativePath = descriptionsSaveFileName + ".TEMP";
            try {
                FileInputStream fileIn = new FileInputStream(tentativePath);
                ObjectMapper objMapper = new ObjectMapper();
                String contents = new String(Files.readAllBytes(Paths.get(tentativePath)));
                systemDescriptions = objMapper.readValue(contents, new TypeReference<TreeMap<Long, String>>() {
                });
                fileIn.close();
            } catch (FileNotFoundException e) {
                success = false;
            } catch (IOException ex) {
                success = false;
            } finally {
            }
            return success;
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
            topic += Long.toString(arg.getPersistentState().getUid());
            try {
                client.publish(topic, messageStr.getBytes(), 1, true);
            } catch (MqttException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            // log("Config pushed. Topic string: " + topic + ", JSON: " + messageStr);
        }
    }

    public void pushDescriptionsToUI() {
        synchronized (descriptionsLock) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                Socket socket = new Socket(CommonValues.localhost, CommonValues.localUIPort);
                PrintWriter tcpOut = new PrintWriter(socket.getOutputStream(), true);
                tcpOut.println(CommonValues.pushDescriptionsToUI);
                String info = mapper.writeValueAsString(systemDescriptions);
                tcpOut.println(info);
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void pushEventsToUI() {
        synchronized (eventsLock) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                Socket socket = new Socket(CommonValues.localhost, CommonValues.localUIPort);
                PrintWriter tcpOut = new PrintWriter(socket.getOutputStream(), true);
                tcpOut.println(CommonValues.pushEventsToUI);
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
    }

    public void pushStateDataToUI() {
        synchronized (proxiesLock) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                Socket socket = new Socket(CommonValues.localhost, CommonValues.localUIPort);
                // Scanner tcpIn = new Scanner(socket.getInputStream());
                PrintWriter tcpOut = new PrintWriter(socket.getOutputStream(), true);
                tcpOut.println(CommonValues.pushProxiesToUI);
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
            handleEmbeddedEvent(splitTopic, message);
        } else if (initialTopic.equals(TopicStrings.embeddedStatePush())) { // We have just been given our periodic status update from one of our systems.
            handleEmbeddedStatePush(splitTopic, message);
        } else if (initialTopic.equals(TopicStrings.stateControlRequest())) {
            //  log("Got state control request");
            handleControllerRequest(message);
        } else if (initialTopic.equals(TopicStrings.descriptionsUpdateRequest())) {
            //  log("Got description update request");
            handleDescriptionUpdate(message);
        } else {
            log("Unknown MQTT topic received: " + topic);
        }
    }

    protected void handleDescriptionUpdate(MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            TreeMap<Long, String> info = objectMapper.readValue(message.toString(), new TypeReference<TreeMap<Long, String>>() {
            });
            for (long k : info.keySet()) {
                String desc = info.get(k);
                synchronized (descriptionsLock) {
                    systemDescriptions.put(k, desc);
                }
                //  log("Description for UID " + k + ": " + desc);

            }
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
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
        synchronized (proxiesLock) {
            if (systems.containsKey(uid)) { // If we know the uid already, we can send to the device any its config info.
                events.add(uid, System.currentTimeMillis(), info);
                ArduinoEventDescriptions descr = new ArduinoEventDescriptions();
                //  log("Event \"" + info + "\" added to log. Device: " + uid);
            } else {
                log("Received event for unknown device  " + uid);
            }
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
        final long uid = info.getPersistentState().getUid();

        info.getTransientState().setTimestamp(System.currentTimeMillis());
        if (systems.containsKey(uid)) {
            if (!liveSystems.contains(uid)) {
                liveSystems.add(uid);
                log("Re-adding " + uid + " to live systems!");
            }
            systems.replace(uid, info);
        } else {
            ArduinoProxy proxy = ArduinoProxySaneDefaultsFactory.get();
            proxy.getPersistentState().setUid(uid);
            systems.put(uid, proxy);
            subscribeToEmbeddedSystem(uid);
            ArduinoConfigChangeRepresentation representation = new ArduinoConfigChangeRepresentation();
            representation.setPersistentState(info.getPersistentState());
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
        // Send the control message to the relevant embedded devices, but validate first.
        for (ArduinoConfigChangeRepresentation req : requestItems) {
            boolean settingLightsOnHour = false;
            boolean settingLightsOffHour = false;
            boolean settingLightsOnMinute = false;
            boolean settingLightsOffMinute = false;
            final int lightsOnHour = req.getPersistentState().getLightsOnHour();
            final int lightsOnMinute = req.getPersistentState().getLightsOnMinute();
            final int lightsOffHour = req.getPersistentState().getLightsOffHour();
            final int lightsOffMinute = req.getPersistentState().getLightsOffMinute();
            final long uid = req.getPersistentState().getUid();
            // Validating time
            if (req.hasChanges()) {
                if (req.isChangingLightsOnHour()) {
                    settingLightsOnHour = true;
                }
                if (req.isChangingLightsOnMinute()) {
                    settingLightsOnMinute = true;
                }
                if (req.isChangingLightsOffHour()) {
                    settingLightsOffHour = true;
                }
                if (req.isChangingLightsOffMinute()) {
                    settingLightsOffMinute = true;
                }
                boolean validOnTime = false;
                if (settingLightsOnHour && settingLightsOnMinute) {
                    validOnTime = TimeOfDayValidator.validate(lightsOnHour, lightsOnMinute);
                } else if (settingLightsOnHour) {
                    validOnTime = TimeOfDayValidator.validate(lightsOnHour, systems.get(uid).getPersistentState().getLightsOnMinute());
                } else if (settingLightsOnMinute) {
                    validOnTime = TimeOfDayValidator.validate(systems.get(uid).getPersistentState().getLightsOnHour(), lightsOnMinute);
                }
                if (!validOnTime) {
                    req.setChangingLightsOnHour(false);
                    req.setChangingLightsOnMinute(false);
                }
                boolean validOffTime = false;
                if (settingLightsOffHour && settingLightsOffMinute) {
                    validOffTime = TimeOfDayValidator.validate(lightsOffHour, lightsOffMinute);
                } else if (settingLightsOffHour) {
                    validOffTime = TimeOfDayValidator.validate(lightsOffHour, systems.get(uid).getPersistentState().getLightsOffMinute());
                } else if (settingLightsOffMinute) {
                    validOffTime = TimeOfDayValidator.validate(systems.get(uid).getPersistentState().getLightsOffHour(), lightsOffMinute);
                }
                if (!validOffTime) {
                    req.setChangingLightsOffHour(false);
                    req.setChangingLightsOffMinute(false);
                }
                // Validating misting interval
                if (req.isChangingMistingInterval() && req.getPersistentState().getMistingInterval() < 0) {
                    req.setChangingMistingInterval(false);
                }
                // Validating misting duration
                if (req.isChangingMistingDuration() && req.getPersistentState().getMistingDuration() < 0) {
                    req.setChangingMistingDuration(false);
                }
                // Validating solution ratio of nutrients vs water
                final double solutionRatio = req.getPersistentState().getNutrientSolutionRatio();
                if (req.isChangingNutrientSolutionRatio() && (solutionRatio > CommonValues.maxNutrientSolutionRatio || solutionRatio < CommonValues.minNutrientSolutionRatio)) {
                    req.setChangingNutrientSolutionRatio(false);
                }
                // Validating humidity
                final float humidity = req.getPersistentState().getTargetUpperChamberHumidity();
                if (req.isChangingTargetUpperChamberHumidity() && (humidity > CommonValues.maxHumidity || humidity < CommonValues.minHumidity)) {
                    req.setChangingTargetUpperChamberHumidity(false);
                }
                // Validating temperature
                final float temperature = req.getPersistentState().getTargetUpperChamberTemperature();
                if (req.isChangingTargetUpperChamberTemperature() && (temperature > CommonValues.maxTargetTemperature || temperature < CommonValues.minTargetTemperature)) {
                    req.setChangingTargetUpperChamberTemperature(false);
                }
                // Validating target CO2 levels
                if (req.isChangingTargetCO2PPM()) {
                    final int ppm = req.getPersistentState().getTargetCO2PPM();
                    if (ppm < CommonValues.minCO2PPM || ppm > CommonValues.maxCO2PPM) {
                        req.setChangingTargetCO2PPM(false);
                    }
                }

                pushConfig(req);
                log("Sending a control packet");
            }
        }
    }

    protected void subscribeToEmbeddedSystem(long uid) {
        String statusReportTopic = TopicStrings.embeddedStatePush();
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
