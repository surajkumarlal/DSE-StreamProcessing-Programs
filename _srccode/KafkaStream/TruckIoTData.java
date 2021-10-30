/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is part of Kafka Stream Java Program 
 * - This is used to Serialise / Deserialise the Input Message / Event from Kafka 
 *  
 */

package com.alfred.mysimulator.truckmovement;

public class TruckIoTData {

	public TruckIoTData() {
		super();
	}

	public TruckIoTData(String deviceId, int vehicleId, int routeId, int driverId, double latitude, double longitude,
			long timestamp, String status) {
		super();
		this.device_id = deviceId;
		this.vehicle_id = vehicleId;
		this.route_id = routeId;
		this.driver_id = driverId;
		this.latitude = latitude;
		this.longitude = longitude;
		this.timestamp = timestamp;
		this.status = status;
	}

	private String device_id;
	private int vehicle_id;
	private int route_id;
	private int driver_id;
	private double latitude;
	private double longitude;
	private long timestamp;
	private String status;

	public String getDevice_id() {
		return device_id;
	}

	public int getVehicle_id() {
		return vehicle_id;
	}

	public int getRoute_id() {
		return route_id;
	}

	public int getDriver_id() {
		return driver_id;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getStatus() {
		return status;
	}

	@Override
	public String toString() {
		return "TruckIoTData [device_id=" + device_id + ", vehicle_id=" + vehicle_id + ", route_id=" + route_id
				+ ", driver_id=" + driver_id + ", latitude=" + latitude + ", longitude=" + longitude + ", timestamp="
				+ timestamp + ", status=" + status + ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
				+ ", toString()=" + super.toString() + "]";
	}
}
