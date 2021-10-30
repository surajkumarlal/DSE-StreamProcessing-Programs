/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is Part of Simulator Program for Truck IoT Data Generator
 * This logs message upon successful posting of events to MQTT (Mosquitto) Broker 
 */
package com.alfred.mysimulator.truckmovement;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;

public class MessageActionListener implements IMqttActionListener {
	protected final String messageText;
	protected final String topic;
	protected final String userContext;

	public MessageActionListener(String topic, String messageText, String userContext) {
		this.topic = topic;
		this.messageText = messageText;
		this.userContext = userContext;
	}

	public void onSuccess(IMqttToken asyncActionToken) {
		if ((asyncActionToken != null) && asyncActionToken.getUserContext().equals(userContext)) {
			System.out.println(String.format("Message '%s' published to topic '%s'", messageText, topic));
		}
	}

	public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
		exception.printStackTrace();
	}
}
