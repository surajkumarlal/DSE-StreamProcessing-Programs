/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is MQTT-Kafka Bridge Program 
 * This reads the message from the MQTT Broker Topic, and then writes the events to Kafka Broker 
 * While writing the message to Kafka Topic, the key is extracted from MQTT message and set while sending across to Kafka
 * The message is not tampered, and continues as JSON with following fields:
 * deviceId, vehicleId, routeId, driverId, gps_lat, gps_long, timestamp, status
 */
package com.alfred.mysimulator.truckmovement;

import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MQTTKafkaBridge implements MqttCallback {
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private MqttAsyncClient mqtt;
	private Producer<String, String> kafkaProducer;

	private void connect(String serverURI, String clientId, String brokerList) throws MqttException {
		mqtt = new MqttAsyncClient(serverURI, clientId);
		mqtt.setCallback(this);
		IMqttToken token = mqtt.connect();

		Properties props = new Properties();
		 props.put("bootstrap.servers", brokerList);
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
		 kafkaProducer = new KafkaProducer<String, String>(props);
		 token.waitForCompletion();
		 logger.info("Connected to MQTT and Kafka");
	}

	private void reconnect() throws MqttException {
		IMqttToken token = mqtt.connect();
		token.waitForCompletion();
	}

	private void subscribe(String[] mqttTopicFilters) throws MqttException {
		int[] qos = new int[mqttTopicFilters.length];
		for (int i = 0; i < qos.length; ++i) {
			qos[i] = 0;
		}
		mqtt.subscribe(mqttTopicFilters, qos);
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.info("Lost connection to MQTT server::" + cause.toString());
		while (true) {
			try {
				logger.info("Attempting to reconnect to MQTT server");
				reconnect();
				logger.info("Reconnected to MQTT server, resuming");
				return;
			} catch (MqttException e) {
				logger.info("Reconnect failed, retrying in 10 seconds::"+ e.toString());
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		
		byte[] payload = message.getPayload();
		String msg = new String(payload);
		String device_id = msg.substring(msg.indexOf("device_id")+"device_id".length()+3, msg.indexOf("vehicle_id")-3);
		System.out.println("mqtt->kafka::"+device_id+"->"+msg);
		//System.out.println("mqtt->kafka::"+msg);
		//kafkaProducer.send(new ProducerRecord<String, String>("mqtt_vehicle_position", msg));
		kafkaProducer.send(new ProducerRecord<String, String>("mqtt_vehicle_position", device_id, msg));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			MQTTKafkaBridge bridge = new MQTTKafkaBridge();
			bridge.connect("tcp://localhost:1883", "mqttKafkaBridge", "localhost:9092");
			bridge.subscribe("vehicle-position".split(","));
		} catch (MqttException e) {
			e.printStackTrace(System.err);
		}
	}
}