/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import noob.plantsystem.common.EmbeddedSystemConfigChange;
import noob.plantsystem.common.EmbeddedSystemEventDescriptions;
import noob.plantsystem.common.EmbeddedSystemProxy;
import noob.plantsystem.common.EmbeddedSystemProxySaneDefaultsFactory;
import noob.plantsystem.common.CommonValues;
import noob.plantsystem.common.ConnectionUtils;
import noob.plantsystem.common.EmbeddedStateChangeValidator;
import noob.plantsystem.common.EventRecord;
import noob.plantsystem.common.PersistentArduinoState;
import noob.plantsystem.common.TopicStrings;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 *
 * @author noob
 */
public class Backend implements MqttCallback {

    protected EventPool events = new EventPool(CommonValues.eventPoolQueueSize);
    protected TreeMap<Long, EmbeddedSystemProxy> proxies = new TreeMap<>();
    protected TreeMap<Long, String> systemDescriptions = new TreeMap<>();
    protected HashSet<Long> liveSystems = new HashSet<>();
    protected EmbeddedSystemEventDescriptions eventDescriptions = new EmbeddedSystemEventDescriptions();
    final Object proxiesLock = new Object();
    final Object descriptionsLock = new Object();
    final Object eventsLock = new Object();
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
        for (Long key : proxies.keySet()) {
            EmbeddedSystemProxy p = proxies.get(key);
            if ((now - p.getTransientState().getTimestamp()) > CommonValues.embeddedSystemTimeout) {
                liveSystems.remove(key);
            }
        }
    }

    public boolean saveSystems() {
        synchronized (proxiesLock) {
            boolean success = true;
            File tentativePath = new File(CommonValues.stateSaveFileName);
            FileOutputStream fileOut = null;
            try {
                fileOut = new FileOutputStream(tentativePath);
                mapper.writeValue(fileOut, proxies);
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
            proxiesLock.notifyAll();
            return success;
        }
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
        synchronized (proxiesLock) {
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
                        proxies = mapper.readValue(contents, new TypeReference<TreeMap<Long, EmbeddedSystemProxy>>() {
                        });
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
            proxiesLock.notifyAll();
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
    public void pushConfig(EmbeddedSystemConfigChange arg) {
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
            String topic = TopicStrings.configPushToEmbedded();
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
            ConnectionUtils.closeConnection(tcpOut, socket);
            descriptionsLock.notifyAll();
        }
    }

    public void pushEventsToUI() {
        synchronized (eventsLock) {
            Socket socket = null;
            PrintWriter tcpOut = null;
            String info = null;
            TreeMap<Long, ArrayDeque<EventRecord>> results = new TreeMap<>();
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
            ConnectionUtils.closeConnection(tcpOut, socket);
            eventsLock.notifyAll();
        }
    }

    public void pushStateDataToUI() {
        synchronized (proxiesLock) {
            Socket socket = null;
            PrintWriter tcpOut = null;
            String info = null;
            TreeMap<Long, EmbeddedSystemProxy> results = new TreeMap<>();
            try {
                socket = new Socket(CommonValues.localhost, CommonValues.localUIPort);
                tcpOut = new PrintWriter(socket.getOutputStream(), true);
                tcpOut.println(CommonValues.pushProxiesToUI);
                for (long key : proxies.keySet()) {
                    if (liveSystems.contains(key)) {
                        results.put(key, proxies.get(key));
                    }
                }
                info = mapper.writeValueAsString(results);
                tcpOut.println(info);
            } catch (IOException ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            }
            ConnectionUtils.closeConnection(tcpOut, socket);
            proxiesLock.notifyAll();
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
        } else if (initialTopic.equals(TopicStrings.embeddedStatePush())) { // We have just been given our periodic status update from one of our proxies.
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
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        saveDescriptions();
    }

    protected void handleEmbeddedEvent(String[] splitTopic, MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        Integer info = null;
        try {
            info = objectMapper.readValue(message.toString(), Integer.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (Exception ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
        }

        long uid = Long.parseLong(splitTopic[1]);

        boolean hasKey = false;
        synchronized (proxiesLock) {
            hasKey = proxies.containsKey(uid);
            proxiesLock.notifyAll();
        }
        synchronized (eventsLock) {
            if (hasKey) { // If we know the uid already, we can send to the device any its config info.
                events.add(uid, System.currentTimeMillis(), info);
                // EmbeddedSystemEventDescriptions descr = new EmbeddedSystemEventDescriptions();
                /// log("Event \"" + info + "\" added to log. Device: " + uid);
            } else {
                log("Received event for unknown device  " + uid);
            }
            eventsLock.notifyAll();
        }
    }

    protected void handleEmbeddedStatePush(String[] splitTopic, MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        EmbeddedSystemProxy info = null;
        try {
            info = objectMapper.readValue(message.toString(), EmbeddedSystemProxy.class);
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        final long uid = info.getPersistentState().getUid();
        info.getTransientState().setTimestamp(System.currentTimeMillis());
        synchronized (proxiesLock) {
            if (proxies.containsKey(uid)) {
                if (!liveSystems.contains(uid)) {
                    liveSystems.add(uid);
                    log("Re-adding " + uid + " to live proxies!");
                    subscribeToEmbeddedSystem(uid);

                }
                proxies.replace(uid, info);
            } else {
                EmbeddedSystemProxy proxy = EmbeddedSystemProxySaneDefaultsFactory.get();
                proxy.getPersistentState().setUid(uid);
                proxies.put(uid, proxy);
                EmbeddedSystemConfigChange representation = new EmbeddedSystemConfigChange();
                representation.setPersistentState(info.getPersistentState());
                representation.changeAll();
                pushConfig(representation);
                saveSystems();
            }
            proxiesLock.notifyAll();
        }
    }

    protected void handleControllerRequest(MqttMessage message) {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayList<EmbeddedSystemConfigChange> requestItems = new ArrayList<>();
        log("Got a request to change state!");
        //readValue(response, new TypeReference<ArrayList<ArduinoProxy>>() {} )
        try {
            requestItems = objectMapper.readValue(message.toString(), new TypeReference<ArrayList<EmbeddedSystemConfigChange>>() {
            });
        } catch (JsonProcessingException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(Backend.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        // Send the control message to the relevant embedded devices, but validate first.
        for (EmbeddedSystemConfigChange req : requestItems) {
            final long uid = req.getPersistentState().getUid();
            if (proxies.containsKey(uid)) {
                PersistentArduinoState current = proxies.get(uid).getPersistentState();
                pushConfig(EmbeddedStateChangeValidator.validate(req, current.getLightsOnHour(), current.getLightsOnMinute(), current.getLightsOffHour(), current.getLightsOffMinute()));
            }
            // log("Sending a control packet");
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
