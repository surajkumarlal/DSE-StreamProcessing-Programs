/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is Simulator Program for Truck IoT Data Generator
 * This emits the events to MQTT (Mosquitto) Broker 
 * The message published is JSON message which includes
 * deviceId, vehicleId, routeId, driverId, gps_lat, gps_long, timestamp, status
 */
package com.alfred.mysimulator.truckmovement;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.google.gson.Gson;


public class TruckDataGenerator {
	private void generateIoTEvent(MQTTProducer mqttp_obj, int vehicleId, int driverId, int routeId,
			double i_latitude, double i_longtude) throws InterruptedException {
		Random rand = new Random();

		String deviceId = UUID.randomUUID().toString().replace("-", "");
		;
		double gps_lat = i_latitude;
		double gps_long = i_longtude;
		String status = "RUNNING";
		int counter = 1;

		int dir_long = 1;
		int dir_lat = 1;
		//Setting Direction Based on Routes
		switch(routeId) {
		  case 10:
		    // code block
			  dir_lat = 0;  
		    break;
		  case 16:
		    // code block
			  dir_long = 0;
		    break;
		  case 22:
			    // code block
			  	dir_long = -1;
			    break;
		  case 28:
			    // code block
			  	dir_lat = -1;
			    break;
		  case 34:
			    // code block
			    break;
		  case 40:
			    // code block
			    break;
		  case 46:
			    // code block
			    break;
		  case 52:
			    // code block
			    break;
		  case 58:
			    // code block
			    break;
		  default:
		    // code block
		}
		
		try {
			while (true) {
				if (counter % 10 == 0) {
					counter = rand.nextInt(10 - 1) + 1;// random speed between 1 to 10
					status = "BROKEN";
				} else {
					counter++;
					status = "RUNNING";
					gps_lat += dir_lat * rand.nextFloat();
					gps_long += dir_long * rand.nextFloat();
				}

				long timestamp = System.currentTimeMillis();
				
				//Preparing Message
				TruckIoTData truck_iotdata = new TruckIoTData(deviceId, vehicleId, routeId, driverId, gps_lat, gps_long, timestamp, status);
				Gson gson = new Gson();  
				String iot_data = gson.toJson(truck_iotdata);
				// System.out.println(iot_data);

				try {
					if (mqttp_obj.isConnected()) {
						mqttp_obj.publishTextMessage(iot_data);
					}
				} catch (Exception ex) {
					System.out.println("Error in connection for " + mqttp_obj.getName() + "," + ex);
					ex.printStackTrace();
				}

				Thread.sleep(rand.nextInt(3000 - 1000) + 1000);// random delay of 1 to 3 seconds
			}
		} catch (Exception ex) {
			System.out.println("Error in connection for " + mqttp_obj.getName() + "," + ex);
			ex.printStackTrace();
		} finally {
			if (mqttp_obj.isConnected()) {
				try {
					mqttp_obj.client.disconnect();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		int vehicles = 5; //Default

		Random rand = new Random();

		List<Integer> routeList = Arrays.asList(new Integer[] { 10, 16, 22, 28, 34, 40, 46, 52, 58, 64 });

		List<String> route_latlong = Arrays.asList(new String[] { "33,-96", "34,-97", "35,-98", "36,-99", "37,-100",
				"38,-101", "39,-101", "40,-102", "40,-103", "41,-104" });

		route_latlong = Arrays.asList(new String[] { "25,78", "23,75", "26,79", "27,80", "25,75",
				"28,70", "26,72", "20,76", "21,84", "22,80" });
		
		if (args.length < 1) {
			System.out.println("Truck IOT Generator Project Usage: jarfilename <# of vehicles>");
			System.out.println("** Starting with default 5 Trucks **");
			//System.exit(2);
		} else if ((args.length == 1) || (args.length > 1)) {
			vehicles = Integer.parseInt(args[0]);
			if (vehicles > 10) {
				System.out.println("Too many vehicles requested, starting 10");
				vehicles = 10;
			} else if (vehicles < 0) {
				System.out.println("Too few vehicles requested, starting 10");
				vehicles = 10;
			}
		}

		for (int i = 0; i < vehicles; i++) {
			final int route_id = routeList.get(i);
			final String truck_name = "Truck-" + Integer.toString(i);
			final int truck_id = i;
			final int driver_id = i;
			String s_route_latlong = route_latlong.get(i);
			String[] strarray = s_route_latlong.split(",");
			final double r_lat = Double.parseDouble(strarray[0]);
			final double r_long = Double.parseDouble(strarray[1]);

			Thread thread = new Thread(truck_name) {
				public void run() {
					System.out.println("Starting Truck->" + getName());
					TruckDataGenerator myTruck = new TruckDataGenerator();
					try {
						MQTTProducer mqttp_obj = new MQTTProducer(getName());
						// Connecting to MQTT Broker
						if (mqttp_obj.connect() == false) {
							System.out.println(
									"Error getting MQTT Broker Connection, Nothing Published! -" + mqttp_obj.getName());
						}

						myTruck.generateIoTEvent(mqttp_obj, truck_id, driver_id, route_id, r_lat, r_long);
					} catch (InterruptedException e) {
						System.out.println("Stopping Truck->" + getName());
						e.printStackTrace();
					}
				}
			};
			thread.start();

		}
	}
}
