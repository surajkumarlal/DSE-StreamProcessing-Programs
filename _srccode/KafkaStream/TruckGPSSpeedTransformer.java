/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is part of Kafka Stream Java Program 
 * - It essentially performs the transformation  
 * - Converts the Input Event / Message and transforms to Output Event / Message Format
 * - In this case, essentially computes the SPEED using State Stored with Previous GPS coordinates and Current GPS coordinates
 * 
 */
package com.alfred.mysimulator.truckmovement;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import com.google.gson.Gson;

public class TruckGPSSpeedTransformer implements ValueTransformer<TruckIoTData, TruckIoTDataWithSpeed> {

	private String stateStoreName;
	private KeyValueStore<String, TruckIoTDataWithSpeed> stateStore;

	TruckGPSSpeedTransformer(final String stateStoreName) {
		this.stateStoreName = stateStoreName;
	}

	// Utility function to compute Distance using 2 GPS coordinates in KM
	private double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
		double theta = lon1 - lon2;
		double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
				+ Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
		dist = Math.acos(dist);
		dist = rad2deg(dist);
		dist = dist * 60 * 1.1515;
		if (unit == 'K') {
			dist = dist * 1.609344;
		} else if (unit == 'N') {
			dist = dist * 0.8684;
		}
		return (dist);
	}

	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	/* :: This function converts decimal degrees to radians : */
	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	private double deg2rad(double deg) {
		return (deg * Math.PI / 180.0);
	}

	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	/* :: This function converts radians to decimal degrees : */
	/* ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: */
	private double rad2deg(double rad) {
		return (rad * 180.0 / Math.PI);
	}

	private TruckIoTData deserialize(String json) {
		Gson gson = new Gson();
		TruckIoTData truckobj = gson.fromJson(json, TruckIoTData.class);
		return truckobj;
	}

	@Override
	public TruckIoTDataWithSpeed transform(TruckIoTData newstate) {
		// System.out.println("*** transform ***");
		// System.out.println(newstate.getDevice_id());
		// System.out.println(newstate);

		TruckIoTDataWithSpeed prevstate = stateStore.get(newstate.getDevice_id());

		if (prevstate != null) {
			// 659.9291454553604/520.0948416590691/1629821595635
			// 660.4454833865166/520.100784599781/1629821597188
			// Computing SPEED by first computing Distance and Time (KM / Time)
			double distancetravelled = distance(prevstate.getLatitude(), prevstate.getLongitude(),
					newstate.getLatitude(), newstate.getLongitude(), 'K');
			double timetaken = (((newstate.getTimestamp() - prevstate.getTimestamp()) / 1000) / 60) / 60;
			// We have added some bias to the formula to get speed value, else we were
			// getting 0
			double speed = distancetravelled / (((newstate.getTimestamp() - prevstate.getTimestamp()) / 1000) * 2.2);
			prevstate = new TruckIoTDataWithSpeed(newstate);
			prevstate.setSpeed(speed);

		} else {
			prevstate = new TruckIoTDataWithSpeed(newstate);
			prevstate.setSpeed(0);
		}
		stateStore.put(newstate.getDevice_id(), prevstate);
		// System.out.println(newstate);
		return prevstate;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(ProcessorContext processorContext) {
		stateStore = (KeyValueStore) processorContext.getStateStore(stateStoreName);
	}
}