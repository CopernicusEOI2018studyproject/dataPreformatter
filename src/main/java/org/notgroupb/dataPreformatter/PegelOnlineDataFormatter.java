package org.notgroupb.dataPreformatter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.notgroupb.formats.PegelOnlineDataPoint;
import org.notgroupb.formats.deserialize.PegelOnlineDataPointDeserializer;
import org.notgroupb.formats.serialize.DataPointSerializer;

public class PegelOnlineDataFormatter {
	
	static void formatPegelOnlineStream(StreamsBuilder builder) {
		// Input Stream
		KStream<String, String> dataStream = builder.stream("PegelOnlineDataSource", Consumed.with(Serdes.String(), Serdes.String()));
		KTable<String, PegelOnlineDataPoint> stationsTable = builder.table("PegelOnlineStations",
				Consumed.with(Serdes.String(), Serdes.serdeFrom(
						new DataPointSerializer<PegelOnlineDataPoint>(),
						new PegelOnlineDataPointDeserializer())
				)
		);
		
		// Output Stream 
		KStream<String, PegelOnlineDataPoint> formattedStream = dataStream
				// Split JSON Array of Measurements into single Measurements
				.flatMap(
						(KeyValueMapper<String, String, Iterable<KeyValue<String,String>>>)
						(k, v) -> {
							ArrayList<KeyValue<String, String>> result = new ArrayList<KeyValue<String, String>>();
							
							// Split JSON Array of measurements into single measurements
							String key = k.substring(6);
							String value = v.replaceAll("\\s","")
											.replaceAll("},\\{", "}%*%{");
							
							value = value.substring(value.indexOf("[")+1)
										 .replaceAll("\\[", "");
							
							String[] splitted = value.split("%\\*%");
							for (String element : splitted) {
								result.add(new KeyValue<String,String>(key, element));
							}
							return result;
				})
				// Join measurement with Station
				.join(stationsTable,
				(ValueJoiner<String,PegelOnlineDataPoint,PegelOnlineDataPoint>)(data, station) -> {
					try {
						JSONObject element = new JSONObject(data);
						
						// Extract and set Timestamp
						String recordTimeString = element.getString("timestamp");
						DateTimeFormatter format = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
						LocalDateTime recordTime = LocalDateTime.parse(recordTimeString, format);
						
						// Return invalid record (to be filtered out later) if Record is older than 8 hours
						// As it has no effect on output
						if (recordTime.isBefore(LocalDateTime.now().minusHours(8))) {
							return new PegelOnlineDataPoint();
						}
						station.setRecordTime(recordTime);
						
						// Extract and set measurement
						Double measurement = element.getDouble("value");
						station.setMeasurement(measurement);
						return station;
					} catch (JSONException e) {
						return new PegelOnlineDataPoint();
					}
				})
				// Filter out invalid DataPoints
				.filterNot(
						(Predicate<String, PegelOnlineDataPoint>)(k,v) -> {
							// Discard all records that do not have a timestamp associated with it
							// As they cannot be used in calculations
							return v.getRecordTime() == null;
						}
				)
				// Rekey to use Station Name as key
				.selectKey((KeyValueMapper<String,PegelOnlineDataPoint,String>)(k,v) -> {
					String key = k;
					return v.getName().toUpperCase();
				});
		
		formattedStream.to("PegelOnlineDataRaw",
				Produced.with(
						Serdes.String(),
						Serdes.serdeFrom(new DataPointSerializer<PegelOnlineDataPoint>(), new PegelOnlineDataPointDeserializer()))
				);
	}

}
