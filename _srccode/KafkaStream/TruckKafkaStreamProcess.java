/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is Kafka Stream Java Program 
 * - It reads the message from Kafka Topic "mqtt_vehicle_position" 
 * - Converts the message to Java Object using Serialisation / Deserialisation (Serde) 
 * - Write the message / event to Cockroach DB
 * - Compute Speed using GPS coordinates (previous and current) using Stateful Processing
 * - Finally writes the JSON message to output topic "mqtt_vehicle_position_speed" with following fields:
 * 
 */
package com.alfred.mysimulator.truckmovement;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class TruckKafkaStreamProcess {

	public static final String INPUT_TOPIC = "mqtt_vehicle_position";
	public static final String OUTPUT_TOPIC = "mqtt_vehicle_position_speed";

	public static void main(String[] args) throws IOException {
		//Generating using app id
		String app_id = "streams-vehicleposition-".concat(new SimpleDateFormat("ddMMyyyy_HHmmss").format(new Date()));
		if (args.length < 1) {
			System.out.println("Truck Kafka Stream Processor Project Usage: jarfilename <Stream Consumer Application ID>");
			System.out.println("** Defaulting Consumer ID = "+ app_id + " **");
		}else {
			if(args[0] != null)
				app_id = "streams-vehicleposition-".concat(args[0]);
			System.out.println("** Consumer ID = "+ app_id + " **");
		}
		Properties props = new Properties();
		props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, app_id);
		props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		Serde<String> stringSerde = Serdes.String();
		Serde<TruckIoTData> truckiotdataSerde = StreamSerdes.TruckIoTDataSerde();
		Serde<TruckIoTDataWithSpeed> truckiotdatawithspeedSerde = StreamSerdes.TruckIoTDataWithSpeedSerde();

		StreamsBuilder builder = new StreamsBuilder();

		String gpsSpeedStateStore = "gps-speed-store";
		KeyValueBytesStoreSupplier storeSupplier = Stores.lruMap(gpsSpeedStateStore, 100);
		StoreBuilder<KeyValueStore<String, TruckIoTDataWithSpeed>> storeBuilder = Stores
				.keyValueStoreBuilder(storeSupplier, Serdes.String(), truckiotdatawithspeedSerde);

		builder.addStateStore(storeBuilder);
		
		//Storing the Event / Message to DB 
		VehiclePositionCockroahDBPersister dbpersister = new VehiclePositionCockroahDBPersister();
		KStream<String, TruckIoTData> stream = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, truckiotdataSerde));
		// Java 8+ example, using lambda expressions
		stream.peek((key, value) -> System.out.println("key=" + key + ", value=" + value));
		stream.peek((key, value) -> dbpersister.insertRecord(value));
		
		//Computing the Speed value using GPS coordinates (Previous and Current) using Stateful Processing / Transformation
		//stream.print(Printed.toSysOut());
		KStream<String, TruckIoTDataWithSpeed> outstream = stream.transformValues(() -> new TruckGPSSpeedTransformer(gpsSpeedStateStore), gpsSpeedStateStore);
		outstream.print(Printed.<String, TruckIoTDataWithSpeed>toSysOut().withLabel("TruckIoTDataWithSpeed"));
		outstream.to(OUTPUT_TOPIC, Produced.with(stringSerde, truckiotdatawithspeedSerde));

		
		final KafkaStreams streams = new KafkaStreams(builder.build(), props);

		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-vehicleposition-shutdown-hook") {
			@Override
			public void run() {
				System.out.print("*** Shutting down stream application ***");
				streams.close();
				latch.countDown();
			}
		});

		try {
			// Resetting the Offset for Local Run Only
			System.out.println("*** Resetting the Offset ***");
			streams.cleanUp();
			System.out.println("*** Starting Stream ***");
			streams.start();
			latch.await();
		} catch (final Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}
