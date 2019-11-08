/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package noob.plantsystem.backend;

/**
 *
 * @author noob
 * Events in this context refers to something that happened in a physical system being monitored.
 * These correspond to actuators being used or sensors that get triggered.
 * Not included in this are processes intended to be transparent to an end-user:
 * Examples of such transparent processes: IP address changes, measurements.
 */
public class EventRecord {
    public int eventType;
    public long timestamp;
}
