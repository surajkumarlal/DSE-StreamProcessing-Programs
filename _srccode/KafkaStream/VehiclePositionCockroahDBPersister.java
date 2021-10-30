/**
 * SPA Assignment 2
 * @author Group 142::SURESH BABASAHEB NIMBALKAR (2019HC04104), WAVHAL HEMANT SUDHIR (2019HC04093), SURAJ KUMAR (2019HC04912)
 * 
 * This is part of Kafka Stream Java Program 
 * - It write the message / event to Cockroach DB Schema spaassignment2.vehicleposition
 * - The table containts columnar data as well as JSON string
 * - fields written includes:
 * DEVICE_ID, VEHICLE_ID, ROUTE_ID, DRIVER_ID, LATITUDE, LONGITUDE, TIMESTAMP, STATUS, MESSAGE
 * 
 */
package com.alfred.mysimulator.truckmovement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

import com.google.gson.Gson;

public class VehiclePositionCockroahDBPersister {
	private final DataSource ds;

	VehiclePositionCockroahDBPersister() {
		// Configure the database connection.
		PGSimpleDataSource ds = new PGSimpleDataSource();
		ds.setServerNames(new String[] { "localhost" });
		ds.setPortNumbers(new int[] { 26257 });
		ds.setDatabaseName("spaassignment2");
		ds.setUser("root");
		ds.setSsl(false);
		ds.setSslMode("disable");
		ds.setReWriteBatchedInserts(true); // add `rewriteBatchedInserts=true` to pg connection string
		ds.setApplicationName("VehiclePositionCockroahDBPersister");

		this.ds = ds;
	}

	public void insertRecord(TruckIoTData data) {
		if (this.ds == null) {
			System.out.println("Datasource not initialised, returning...");
			return;
		}

		String sqlCode = "insert into vehicleposition(DEVICE_ID, VEHICLE_ID, ROUTE_ID, DRIVER_ID, LATITUDE, LONGITUDE, TIMESTAMP, STATUS, MESSAGE) values (?, ? , ?, ?, ?, ?, ?, ?, ?)";

		String DEVICE_ID = data.getDevice_id();
		int VEHICLE_ID = data.getVehicle_id();
		int ROUTE_ID = data.getRoute_id();
		int DRIVER_ID = data.getDriver_id();
		double LATITUDE = data.getLatitude();
		double LONGITUDE = data.getLongitude();
		long TIMESTAMP = data.getTimestamp();
		String STATUS = data.getStatus();
		Gson gson = new Gson();
		String MESSAGE = gson.toJson(data);

		// System.out.println(MESSAGE);

		try (Connection connection = ds.getConnection()) {
			// We're managing the commit lifecycle ourselves so we can
			// automatically issue transaction retries.
			connection.setAutoCommit(false);

			try (PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {

				try {
					pstmt.setString(1, DEVICE_ID);
					pstmt.setInt(2, VEHICLE_ID);
					pstmt.setInt(3, ROUTE_ID);
					pstmt.setInt(4, DRIVER_ID);
					pstmt.setDouble(5, LATITUDE);
					pstmt.setDouble(6, LONGITUDE);
					pstmt.setLong(7, TIMESTAMP);
					pstmt.setString(8, STATUS);
					pstmt.setString(9, MESSAGE);
				} catch (NumberFormatException e) {
					e.printStackTrace();
				}

				if (pstmt.execute()) {
					System.out.println("Prepared statement execution return True");
				} else {
					int updateCount = pstmt.getUpdateCount();
					System.out.println("Prepared statement execution return False::" + Integer.toString(updateCount));
				}

				connection.commit();
				System.out.println("Transaction Committed!");
			} catch (SQLException e) {
				System.out.printf("runSQL ERROR: { state => %s, cause => %s, message => %s }\n", e.getSQLState(),
						e.getCause(), e.getMessage());
			}
		} catch (SQLException e) {
			System.out.printf("runSQL ERROR: { state => %s, cause => %s, message => %s }\n", e.getSQLState(),
					e.getCause(), e.getMessage());
		}
	}
}