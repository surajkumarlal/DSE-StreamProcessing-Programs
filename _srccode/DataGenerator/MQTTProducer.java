/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is Part of Simulator Program for Truck IoT Data Generator
 * This emits the events to MQTT (Mosquitto) Broker 
 * The message published is JSON message which includes
 * deviceId, vehicleId, routeId, driverId, gps_lat, gps_long, timestamp, status
 */
package com.alfred.mysimulator.truckmovement;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ThreadLocalRandom;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.UUID;

public class MQTTProducer implements IMqttActionListener {
	// Replace with your own topic name
	public static final String TOPIC = "vehicle-position";

	public static final String ENCODING = "UTF-8";

	// Quality of Service = Exactly once
	// I want to receive all messages exactly once
	public static final int QUALITY_OF_SERVICE = 2;
	protected String name;
	protected String clientId;
	protected MqttAsyncClient client;
	protected MemoryPersistence memoryPersistence;
	protected IMqttToken connectToken;
	protected IMqttToken subscribeToken;

	public MQTTProducer(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public boolean connect() {
		try {
			MqttConnectOptions conOpt = new MqttConnectOptions();
			conOpt.setCleanSession(true);
			// options.setUserName(
			// "replace with your username");
			// options.setPassword(
			// "replace with your password"
			// .toCharArray());
			// Replace with ssl:// and work with TLS/SSL
			// best practices in a
			// production environment
			memoryPersistence = new MemoryPersistence();
			String serverURI = "tcp://iot.eclipse.org:1883"; //If you want to use the cloud MQTT Broker

			serverURI = "tcp://localhost:1883"; // For our program, we are connecting to Broker on local machine

			clientId = MqttAsyncClient.generateClientId();
			client = new MqttAsyncClient(serverURI, clientId, memoryPersistence);
			// In case we can to process the messages produced as the callback
			// client.setCallback(this);
			connectToken = client.connect(conOpt, null, this);
		} catch (MqttException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public boolean isConnected() {
		return (client != null) && (client.isConnected());
	}

	public void connectionLost(Throwable cause) {
		// The MQTT client lost the connection
		System.out.println("Connection lost because: " + cause);
		cause.printStackTrace();
	}

	public void onSuccess(IMqttToken asyncActionToken) {
		if (asyncActionToken.equals(connectToken)) {
			System.out.println(String.format("%s IoT successfully connected to MQTT Broker", name));
		}
	}

	public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
		// The method will run if an operation failed
		exception.printStackTrace();
	}

	public MessageActionListener publishTextMessage(String messageText) {
		byte[] bytesMessage;
		try {
			bytesMessage = messageText.getBytes(ENCODING);
			MqttMessage message;
			message = new MqttMessage(bytesMessage);
			String userContext = "Truck-IOT";
			MessageActionListener actionListener = new MessageActionListener(TOPIC, messageText, userContext);
			client.publish(TOPIC, message, userContext, actionListener);
			return actionListener;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		} catch (MqttException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
		// Delivery for a message has been completed
		// and all acknowledgments have been received
	}
}
