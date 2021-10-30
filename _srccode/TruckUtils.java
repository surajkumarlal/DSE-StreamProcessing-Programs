package com.alfred.mysimulator.truckmovement;

public class TruckUtils {
		private static double distance(double lat1, double lon1, double lat2, double lon2, char unit) {
		  double theta = lon1 - lon2;
		  double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
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

		/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
		/*::  This function converts decimal degrees to radians             :*/
		/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
		private static double deg2rad(double deg) {
		  return (deg * Math.PI / 180.0);
		}

		/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
		/*::  This function converts radians to decimal degrees             :*/
		/*:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/
		private static double rad2deg(double rad) {
		  return (rad * 180.0 / Math.PI);
		}

		public static void main(String[] args) {
			System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, 'M') + " Miles\n");
			System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, 'K') + " Kilometers\n");
			System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, 'N') + " Nautical Miles\n");
			
			double b_lat = 32.9697d;
			double b_long = -96.80322d;
			
			double b_lat_new = 32.9697d;
			double b_long_new = -96.80322d;

			for(int i=0; i<10; i++) {
				b_lat = b_lat_new;
				b_long = b_long_new;
				b_lat_new = b_lat + Math.random();
				b_long_new = b_long_new + Math.random();
				System.out.println(Double.toString(b_lat)+","+ Double.toString(b_long)+","+ Double.toString(b_lat_new)+","+ Double.toString(b_long_new));
				System.out.println(distance(b_lat, b_long, b_lat_new, b_long_new, 'K') + " Kilometers\n");
			}
			
			//659.9291454553604/520.0948416590691/1629821595635
			//660.4454833865166/520.100784599781/1629821597188
			double distancetravelled = distance(659.9291454553604, 520.0948416590691, 660.4454833865166, 520.100784599781, 'K');
			double timetaken = ((1629821597188L - 1629821595635L)/1000)*2;
			System.out.println(distancetravelled);
			//System.out.println((1629821597188L - 1629821595635L));
			System.out.println(timetaken);
			System.out.println(distancetravelled / timetaken);
			
			String msg = "{\"device_id\":\"2a5a47dff99a48f99b6e8ebb43f2369c\",\"vehicle_id\":3,\"route_id\":28,\"driver_id\":3,\"latitude\":41.6759757399559,\"longitude\":-94.90431815385818,\"timestamp\":1629977035574,\"status\":\"RUNNING\"}";
			String device = msg.substring(msg.indexOf("device_id")+"device_id".length()+3, msg.indexOf("vehicle_id")-3);
			System.out.println(device);
			
		}
}
