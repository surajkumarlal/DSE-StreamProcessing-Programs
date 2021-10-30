/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is part of Kafka Stream Java Program 
 * - It essentially used for Serialisation / De-serialisation
 * 
 */
package com.alfred.mysimulator.truckmovement;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer<T> implements Serializer<T> {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void configure(Map<String, ?> props, boolean isKey) {
		// nothing to do
	}

	@Override
	public byte[] serialize(String topic, T data) {
		if (data == null)
			return null;

		try {
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			throw new SerializationException("Error serializing JSON message", e);
		}
	}

	@Override
	public void close() {
		// nothing to do
	}

}
