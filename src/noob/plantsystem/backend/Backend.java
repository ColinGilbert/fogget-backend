/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import noob.plantsystem.common.EmbeddedSystemConfigChangeMemento;
import noob.plantsystem.common.EmbeddedSystemEventDescriptions;
import noob.plantsystem.common.EmbeddedSystemCombinedStateMemento;
import noob.plantsystem.common.EmbeddedSystemStateSaneDefaultsFactory;
import noob.plantsystem.common.CommonValues;
import noob.plantsystem.common.ConnectionCloser;
import noob.plantsystem.common.EmbeddedStateChangeValidator;
import noob.plantsystem.common.EventRecordMemento;
import noob.plantsystem.common.PersistentEmbeddedSystemStateMemento;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import noob.plantsystem.common.EmbeddedSystemEventType;

/**
 *
 * @author noob
 */
public class Backend implements MqttCallback {

    protected EventPool events = new EventPool(CommonValues.eventPoolQueueSize);
    protected TreeMap<Long, EmbeddedSystemCombinedStateMemento> systemsRepresentation = new TreeMap<>();
    protected TreeMap<Long, String> systemDescriptions = new TreeMap<>();
    protected HashSet<Long> liveSystems = new HashSet<>();
    protected EmbeddedSystemEventDescriptions eventDescriptions = new EmbeddedSystemEventDescriptions();
    protected final Object systemsRepresentationLock = new Object();
    protected final Object descriptionsLock = new Object();
    protected final Object eventsLock = new Object();
    protected long currentTime;
    //MQTT related
    protected String brokerURL = CommonValues.mqttBrokerURL;
    protected MqttAsyncClient client;
    protected boolean logging;
    protected MqttConnectOptions connectionOptions;
    protected Object waiter = new Object();
    protected ObjectMapper mapper = new ObjectMapper();

    public void init() throws MqttException {
        connectionOptions = new MqttConnectOptions();
        connectionOptions.setCleanSession(true);
        connectionOptions.setMaxInflight(10000);
        client = new MqttAsyncClient(brokerURL, MqttAsyncClient.generateClientId(), new MemoryPersistence());
        currentTime = System.currentTimeMillis();
        client.setCallback(this);
        loadSystems();
        loadDescriptions();
    }

    void setLogging(boolean arg) {
        logging = arg;
    }

  //  pushEventFromEmbeddedTopic = "pushEventFromEmbedded";
  //  pushStatusToBackendTopic = "pushStatusToBackendTopic";
  //  pushConfigToEmbeddedTopic = "pushConfigToEmbedded";
  //  updateDescriptionRequestTopic = "updateDescriptionRequest";
  //  configEmbeddedRequestTopic = "configEmbeddedRequest";
    
    
    
    public void connect() {
        MqttConnector con = new MqttConnector();
        con.doConnect();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }
        subscribe(CommonValues.configEmbeddedRequestTopic, 1);
        subscribe(CommonValues.updateDescriptionRequestTopic, 1);
        subscribe(CommonValues.pushStatusToBackendTopic, 0);
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
        for (Long key : systemsRepresentation.keySet()) {
            EmbeddedSystemCombinedStateMemento p = systemsRepresentation.get(key);
            if ((now - p.getTransientState().getTimestamp()) > CommonValues.embeddedSystemTimeout) {
                liveSystems.remove(key);
            }
        }
    }

    public boolean saveSystems() {
            
            TreeMap<Long, PersistentEmbeddedSystemStateMemento> toSave = new TreeMap<>();
            synchronized (systemsRepresentationLock) {
            for (long k: systemsRepresentation.keySet()) {
                toSave.put(k, systemsRepresentation.get(k).getPersistentState());
            }
            systemsRepresentationLock.notifyAll();
        }
            boolean success = true;
            File tentativePath = new File(CommonValues.stateSaveFileName);
            FileOutputStream fileOut = null;
            try {
                fileOut = new FileOutputStream(tentativePath);
                mapper.writeValue(fileOut, toSave);
                fileOut.flush();
                success = tentativePath.renameTo(new File(CommonValues.stateSaveFileName));
                fileOut.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                success = false;
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                success = false;
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            return success;
        }
    

    public boolean saveDescriptions() {
        synchronized (descriptionsLock) {
            boolean success = true;
            FileOutputStream fileOut = null;
            File tentativePath = new File(CommonValues.descriptionsSaveFileName);
            try {
                fileOut = new FileOutputStream(tentativePath);
                mapper.writeValue(fileOut, systemDescriptions);
                fileOut.flush();
                success = tentativePath.renameTo(new File(CommonValues.descriptionsSaveFileName));
                fileOut.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                success = false;
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                success = false;
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            descriptionsLock.notifyAll();
            return success;
        }
    }

    public boolean loadSystems() {
        synchronized (systemsRepresentationLock) {
            FileInputStream fileIn = null;
            boolean success = true;
            // String tentativePath = CommonValues.stateSaveFileName;
            Path tentativePath = Paths.get(CommonValues.stateSaveFileName);
            if (Files.exists(tentativePath)) {
                String contents = null;
                try {
                    fileIn = new FileInputStream(tentativePath.toString());
                    if (fileIn.available() > 0) {
                        contents = new String(Files.readAllBytes(tentativePath));
                        TreeMap<Long, PersistentEmbeddedSystemStateMemento> fromSave = new TreeMap<>();
                        fromSave = mapper.readValue(contents, new TypeReference<TreeMap<Long, PersistentEmbeddedSystemStateMemento>>() {
                        });
                        for (long k : fromSave.keySet())
                        {
                            EmbeddedSystemCombinedStateMemento memento = new EmbeddedSystemCombinedStateMemento();
                            memento.setPersistentState(fromSave.get(k));
                            systemsRepresentation.put(k, memento);
                        }
                        fileIn.close();
                    } else {
                        success = false;
                    }
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                    success = false;
                } catch (IOException ex) {
                    Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                    success = false;
                } catch (Exception ex) {
                    Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);

                }
            }
            systemsRepresentationLock.notifyAll();
            return success;
        }
    }

    public boolean loadDescriptions() {
        synchronized (descriptionsLock) {
            boolean success = true;
            FileInputStream fileIn = null;
            String contents = null;
            Path tentativePath = Paths.get(CommonValues.descriptionsSaveFileName);
            if (Files.exists(tentativePath)) {
                try {
                    fileIn = new FileInputStream(tentativePath.toString());
                    contents = new String(Files.readAllBytes(tentativePath));
                    systemDescriptions = mapper.readValue(contents, new TypeReference<TreeMap<Long, String>>() {
                    });
                    fileIn.close();
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                    success = false;
                } catch (IOException ex) {
                    Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                    success = false;
                } catch (Exception ex) {
                    Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                    success = false;
                }
            }
            descriptionsLock.notifyAll();
            return success;
        }
    }

    // Logic to push configuration to embedded system.
    public void pushConfig(EmbeddedSystemConfigChangeMemento arg) {
        if (arg.hasChanges()) { // This prevents us from wasting MQTT requests on empty items.
            log("Pushing configuration");
            String messageStr = null;
            try {
                messageStr = mapper.writeValueAsString(arg);
            } catch (JsonProcessingException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
            String topic = CommonValues.pushConfigToEmbeddedTopic;
            topic += "/";
            topic += Long.toString(arg.getPersistentState().getUid());
            try {
                client.publish(topic, messageStr.getBytes(), 1, true);
            } catch (MqttException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            // log("Config pushed. Topic string: " + topic + ", JSON: " + messageStr);
        }
    }

    public void pushDescriptionsToUI() {
        synchronized (descriptionsLock) {
            Socket socket = null;
            PrintWriter tcpOut = null;
            String info = null;
            try {
                socket = new Socket(CommonValues.localhost, CommonValues.localUIPort);
                tcpOut = new PrintWriter(socket.getOutputStream(), true);
                tcpOut.println(CommonValues.pushDescriptionsToUI);
                info = mapper.writeValueAsString(systemDescriptions);
                tcpOut.println(info);
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            ConnectionCloser.closeConnection(tcpOut, socket);
            descriptionsLock.notifyAll();
        }
    }

    public void pushEventsToUI() {
        synchronized (eventsLock) {
            Socket socket = null;
            PrintWriter tcpOut = null;
            String info = null;
            TreeMap<Long, ArrayDeque<EventRecordMemento>> results = new TreeMap<>();
            try {
                socket = new Socket(CommonValues.localhost, CommonValues.localUIPort);
                tcpOut = new PrintWriter(socket.getOutputStream(), true);
                tcpOut.println(CommonValues.pushEventsToUI);
                for (long key : events.getRaw().keySet()) {
                    // Add sync loop?
                    if (liveSystems.contains(key)) {
                        results.put(key, events.getRaw().get(key));
                    }
                }
                info = mapper.writeValueAsString(results);
                tcpOut.println(info);
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            ConnectionCloser.closeConnection(tcpOut, socket);
            eventsLock.notifyAll();
        }
    }

    public void pushStateDataToUI() {
        synchronized (systemsRepresentationLock) {
            Socket socket = null;
            PrintWriter tcpOut = null;
            String info = null;
            TreeMap<Long, EmbeddedSystemCombinedStateMemento> results = new TreeMap<>();
            try {
                socket = new Socket(CommonValues.localhost, CommonValues.localUIPort);
                tcpOut = new PrintWriter(socket.getOutputStream(), true);
                tcpOut.println(CommonValues.pushSystemsToUI);
                for (long key : systemsRepresentation.keySet()) {
                    if (liveSystems.contains(key)) {
                        results.put(key, systemsRepresentation.get(key));
                    }
                }
                info = mapper.writeValueAsString(results);
                tcpOut.println(info);
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            ConnectionCloser.closeConnection(tcpOut, socket);
            systemsRepresentationLock.notifyAll();
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
        if (initialTopic.equals(CommonValues.pushEventFromEmbeddedTopic)) { // We have been just informed of an event that is worth logging...
            if (splitTopic.length < 2) {
                log("No uid in topic string for embedded event message.");
                return;
            }
            Integer info = null;
            try {
                info = mapper.readValue(message.toString(), Integer.class);
            } catch (JsonProcessingException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
            long uid = 0;
            try {
                uid = Long.parseLong(splitTopic[1]);
            } catch (NumberFormatException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
            handleEmbeddedEvent(uid, info);

        } else if (initialTopic.equals(CommonValues.pushStatusToBackendTopic)) { // We have just been given our periodic status update from one of our systemsRepresentation.
            EmbeddedSystemCombinedStateMemento info = null;
            try {
                info = mapper.readValue(message.toString(), EmbeddedSystemCombinedStateMemento.class);
            } catch (JsonProcessingException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
            final long uid = info.getPersistentState().getUid();
            info.getTransientState().setTimestamp(System.currentTimeMillis());
            handleEmbeddedStatePush(uid, info);
        } else if (initialTopic.equals(CommonValues.configEmbeddedRequestTopic)) {
            //  log("Got state control request");
            ArrayList<EmbeddedSystemConfigChangeMemento> requestItems = null;
            log("Got a request to change state!");
            //readValue(response, new TypeReference<ArrayList<ArduinoProxy>>() {} )
            try {
                requestItems = mapper.readValue(message.toString(), new TypeReference<ArrayList<EmbeddedSystemConfigChangeMemento>>() {
                });
            } catch (JsonProcessingException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
            handleControllerRequest(requestItems);
        } else if (initialTopic.equals(CommonValues.updateDescriptionRequestTopic)) {
            try {
                TreeMap<Long, String> info = mapper.readValue(message.toString(), new TypeReference<TreeMap<Long, String>>() {
                });
                //  log("Got description update request");
                handleDescriptionUpdate(info);
            } catch (JsonProcessingException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            log("Unknown MQTT topic received: " + topic);
        }
    }

    protected void handleDescriptionUpdate(TreeMap<Long, String> info) {
        String desc = null;
        synchronized (descriptionsLock) {
            for (long k : info.keySet()) {
                desc = info.get(k);
                if (desc.length() < CommonValues.maxDescriptionLength + 1) {
                    systemDescriptions.put(k, desc);
                }
            }
            //  log("Description for UID " + k + ": " + desc);
            descriptionsLock.notifyAll();
        }
        saveDescriptions();
    }

    protected void handleEmbeddedEvent(long uid, int info) {
        boolean hasKey = false;
        synchronized (systemsRepresentationLock) {
            hasKey = systemsRepresentation.containsKey(uid);
            systemsRepresentationLock.notifyAll();
        }
        if (hasKey) { // If we know the uid already, we can send to the device any its config info.
            synchronized (eventsLock) {
                events.add(uid, System.currentTimeMillis(), info);
                // EmbeddedSystemEventDescriptions descr = new EmbeddedSystemEventDescriptions();
                /// log("Event \"" + info + "\" added to log. Device: " + uid);
                eventsLock.notifyAll();
            }
        } else {
            log("Received event for unknown device  " + uid);
        }
    }

    protected void handleEmbeddedStatePush(long uid, EmbeddedSystemCombinedStateMemento info) {
        synchronized (systemsRepresentationLock) {
            if (systemsRepresentation.containsKey(uid)) {
                if (!liveSystems.contains(uid)) {
                    liveSystems.add(uid);
                    log("Re-adding " + uid + " to live systemsRepresentation!");
                    subscribeToEmbeddedSystem(uid);
                }
                systemsRepresentation.replace(uid, info);
            } else {
                EmbeddedSystemCombinedStateMemento proxy = EmbeddedSystemStateSaneDefaultsFactory.get();
                proxy.getPersistentState().setUid(uid);
                systemsRepresentation.put(uid, proxy);
                EmbeddedSystemConfigChangeMemento representation = new EmbeddedSystemConfigChangeMemento();
                representation.setPersistentState(info.getPersistentState());
                representation.changeAll();
                pushConfig(representation);
                saveSystems();
            }
            systemsRepresentationLock.notifyAll();
        }
    }

    protected void handleControllerRequest(ArrayList<EmbeddedSystemConfigChangeMemento> requestItems) {
        // Send the control message to the relevant embedded devices, but validate first.
        for (EmbeddedSystemConfigChangeMemento req : requestItems) {
            final long uid = req.getPersistentState().getUid();
            if (systemsRepresentation.containsKey(uid)) {
                PersistentEmbeddedSystemStateMemento current = systemsRepresentation.get(uid).getPersistentState();
                pushConfig(EmbeddedStateChangeValidator.validate(req, current.getLightsOnHour(), current.getLightsOnMinute(), current.getLightsOffHour(), current.getLightsOffMinute()));
            }
            // log("Sending a control packet");
        }
    }

    protected void subscribeToEmbeddedSystem(long uid) {
        String statusReportTopic = CommonValues.pushStatusToBackendTopic;
        statusReportTopic += "/";
        statusReportTopic += uid;
        subscribe(statusReportTopic, 0);
        String eventTopic = CommonValues.pushEventFromEmbeddedTopic;
        eventTopic += "/";
        eventTopic += uid;
        subscribe(eventTopic, 1);
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
