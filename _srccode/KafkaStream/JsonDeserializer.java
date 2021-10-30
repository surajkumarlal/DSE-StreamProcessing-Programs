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
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer<T> implements Deserializer<T> {
	private final ObjectMapper objectMapper = new ObjectMapper();

	private Class<T> tClass;

	public JsonDeserializer() {
	}

	public JsonDeserializer(Class<T> tClass) {
		this.tClass = tClass;
	}

	@Override
	public void configure(Map<String, ?> props, boolean isKey) {
		// nothing to do
	}

	@Override
	public T deserialize(String topic, byte[] bytes) {
		if (bytes == null)
			return null;

		T data;
		try {
			data = objectMapper.readValue(bytes, tClass);
		} catch (Exception e) {
			throw new SerializationException(e);
		}

		return data;
	}

	@Override
	public void close() {
		// nothing to do
	}
}