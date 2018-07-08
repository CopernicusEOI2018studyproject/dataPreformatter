package org.notgroupb.dataPreformatter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.cts.CRSFactory;
import org.cts.crs.CoordinateReferenceSystem;
import org.cts.crs.GeodeticCRS;
import org.cts.op.CoordinateOperation;
import org.cts.op.CoordinateOperationFactory;
import org.cts.registry.EPSGRegistry;
import org.cts.registry.RegistryManager;
import org.notgroupb.formats.HygonDataPoint;
import org.notgroupb.formats.deserialize.HygonDataPointDeserializer;
import org.notgroupb.formats.serialize.DataPointSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HygonDataFormatter {

	final static Logger logger = LoggerFactory.getLogger(HygonDataFormatter.class);

	static void  preformatHygonStream(StreamsBuilder builder) {
		// Input Data Stream
		// Aggregate Data points and only keep latest Measurement
		KStream<byte[], String> rawdataStream = builder.stream("HygonWLRaw");

		// Preformats Data Stream to use Station Name as Key
		KStream<String, String> preformattedDataStream = rawdataStream.map(
				(KeyValueMapper<byte[], String, KeyValue<String,String>>)
				(k, v) -> {
					String key = v.split(";")[0];
					// Somehow first 5 digits of the String get broken into invalid characters. Removing them manually
					return new KeyValue<>(key.toUpperCase().substring(6), v.substring(6));
				}
		);
		preformattedDataStream.to("PreformattedHygonData",
				Produced.with(
						Serdes.String(),
						Serdes.String())
				);

		// Input Stations Stream
		KStream<byte[], String> rawStationsStream = builder.stream("HygonStationsWL");	
		// Preformats Stations Stream to use Station Name as Key
		KStream<String, String> preformattedStationsStream = rawStationsStream.map(
				(KeyValueMapper<byte[], String, KeyValue<String,String>>)
				(k, v) -> {
					String key = v.split(";")[0].substring(6);
					String value = v.substring(6);
					// Removing invalid/broken Characters
					key = (Character.isUpperCase(key.codePointAt(0)))? key : key.substring(1);
					value = (Character.isUpperCase(value.codePointAt(0)))? value : value.substring(1);
					return new KeyValue<>(key.toUpperCase(), value);
				}
		);
		preformattedStationsStream.to("PreformattedHygonStations", 
				Produced.with(
						Serdes.String(),
						Serdes.String())
				);
	}

	/*
	 * Joins the Preformatted Hygon Streams to associate Measurements with Stations
	 * Discards invalid/irrelevant Data Points. A Point is invalid if:
	 * 	- no measurement supplied
	 *  - coordinates could no be transformed to WGS84
	 *  - malformed
	 *  - older than 8 hours
	 */
	static void formatHygonStream(StreamsBuilder builder) throws Exception { 

		// Reimport Stations as Table
		KTable<String,String> stationsTable = builder.table("PreformattedHygonStations", Consumed.with(Serdes.String(), Serdes.String()));
		KStream<String,String> hygonDataStream = builder.stream("PreformattedHygonData", Consumed.with(Serdes.String(), Serdes.String()));

		// Setup Library for Coordinate Transformations
		CRSFactory cRSFactory = new CRSFactory();
		RegistryManager registryManager = cRSFactory.getRegistryManager();
		registryManager.addRegistry(new EPSGRegistry());

		CoordinateReferenceSystem utm32Ncrs = cRSFactory.getCRS("EPSG:32632");
		CoordinateReferenceSystem wgs84crs = cRSFactory.getCRS("EPSG:4326");
		Set<CoordinateOperation> transformations = CoordinateOperationFactory.createCoordinateOperations(
				(GeodeticCRS) utm32Ncrs,
				(GeodeticCRS) wgs84crs);
		
		// Define OutputStream
		KStream<String, HygonDataPoint> outputStream = hygonDataStream.join(stationsTable, 
				(ValueJoiner<String,String,HygonDataPoint>)(data, station) -> {
					try {
						String[] dataArray = data.split(";");
						String[] stationArray = station.split(";");

						HygonDataPoint dp = new HygonDataPoint();
						try {
							
							// Return invalid record (to be filtered out later) if no measurement is supplied
							if (dataArray.length <4) {
								return new HygonDataPoint();
							}
							
							// Extract Date
							DateTimeFormatter format = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
							LocalDateTime recordTime = LocalDateTime.parse(dataArray[1], format);
							
							// Return invalid record (to be filtered out later) if Record is older than 8 hours
							// As it has no effect on output
							if (recordTime.isBefore(LocalDateTime.now().minusHours(8))) {
								return new HygonDataPoint();
							}
							
							// Set Record Time
							dp.setRecordTime(recordTime);
							// Set Measurement
							dp.setMeasurement(Double.parseDouble(dataArray[3]));
							// Set Name
							dp.setName(dataArray[0]);							
							// Set Characteristic Values
							if (!stationArray[2].equals("")) {
								dp.setMnw(Float.parseFloat(stationArray[2]));
							}
							if (!stationArray[3].equals("")) {
								dp.setMhw(Float.parseFloat(stationArray[3]));
							}
							if (!stationArray[4].equals("")) {
								dp.setAverage(Float.parseFloat(stationArray[4]));
							}
							if (!stationArray[5].equals("")) {
								dp.setLevel1(Float.parseFloat(stationArray[5]));
							}
							if (!stationArray[6].equals("")) {
								dp.setLevel2(Float.parseFloat(stationArray[6]));
							}
							if (!stationArray[7].equals("")) {
								dp.setLevel3(Float.parseFloat(stationArray[7]));
							}

							// Transform coordinates from EPSG:32632 to EPDS:4326 (WGS84)
							if (transformations.size() != 0) {
								for (CoordinateOperation op : transformations) {
									// Transform coord using the op CoordinateOperation from crs1 to crs2
									double[] wgs84coords  = op.transform(new double[] {
											Float.parseFloat(stationArray[9]),
											Float.parseFloat(stationArray[10])});
									dp.setLon(wgs84coords[0]);
									dp.setLat(wgs84coords[1]);
									break;
								}
							} else {
								logger.error("Could not find any transformation from UTM32N to WGS84");
								throw new Exception("Could not find any transformation from UTM32N to WGS84");
							}

						} catch (Exception e) {
							// e.printStackTrace();
							logger.error("Error parsing Data to HygonDataPoint. Exception was:" + e + e.getMessage());
							return new HygonDataPoint();
						}
						return dp;
					} catch (Exception e) {
						logger.error("Error merging Preformatted HygonDataStreams. Exception was:" + e);
						return new HygonDataPoint();
					} 
				}
				).filterNot(
				(Predicate<String, HygonDataPoint>)(k,v) -> {
					// Discard all records that do not have a timestamp associated with it
					// As they cannot be used in calculations
					return v.getRecordTime() == null;
				}
				);
		outputStream.to("HygonDataRaw",
				Produced.with(
						Serdes.String(),
						Serdes.serdeFrom(new DataPointSerializer<HygonDataPoint>(), new HygonDataPointDeserializer()))
				);
	}
}
