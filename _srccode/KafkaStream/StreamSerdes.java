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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class StreamSerdes {
	public static Serde<TruckIoTData> TruckIoTDataSerde() {
		return new TruckIoTDataSerde();
	}

	public static final class TruckIoTDataSerde extends WrapperSerde<TruckIoTData> {
		public TruckIoTDataSerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>(TruckIoTData.class));
		}
	}

	public static Serde<TruckIoTDataWithSpeed> TruckIoTDataWithSpeedSerde() {
		return new TruckIoTDataWithSpeedSerde();
	}

	public static final class TruckIoTDataWithSpeedSerde extends WrapperSerde<TruckIoTDataWithSpeed> {
		public TruckIoTDataWithSpeedSerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>(TruckIoTDataWithSpeed.class));
		}
	}

	private static class WrapperSerde<T> implements Serde<T> {

		private JsonSerializer<T> serializer;
		private JsonDeserializer<T> deserializer;

		WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
			this.serializer = serializer;
			this.deserializer = deserializer;
		}

		@Override
		public void configure(Map<String, ?> map, boolean b) {

		}

		@Override
		public void close() {

		}

		@Override
		public Serializer<T> serializer() {
			return serializer;
		}

		@Override
		public Deserializer<T> deserializer() {
			return deserializer;
		}
	}
}
