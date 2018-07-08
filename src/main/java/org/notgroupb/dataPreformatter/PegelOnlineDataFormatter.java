package org.notgroupb.dataPreformatter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.notgroupb.formats.PegelOnlineDataPoint;
import org.notgroupb.formats.deserialize.PegelOnlineDataPointDeserializer;
import org.notgroupb.formats.serialize.DataPointSerializer;

public class PegelOnlineDataFormatter {
	static void formatPegelOnlineStream(StreamsBuilder builder) {
		// Input Stream
		KStream<String, String> dataStream = builder.stream("PegelOnlineData");
		KTable<String, PegelOnlineDataPoint> stationsTable = builder.table("PegelOnlineStations");
		
		dataStream.print();
		stationsTable.print();
		
		// Output Stream 
		KStream<String, PegelOnlineDataPoint> formattedStream = dataStream
				// Split into single measurements
				.flatMapValues(
				(ValueMapper<String, ? extends Iterable<? extends String>>) (v) -> {
					v = v.replaceAll("\\s","").substring(1, v.length()-1);
					String[] result = v.split(",");
					return Arrays.asList(result);
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
						station.setRecordTime(recordTime);
						
						// Extract and set measurement
						Double measurement = element.getDouble("value");
						station.setMeasurement(measurement);
						return station;
					} catch (JSONException e) {
						e.printStackTrace();
						return new PegelOnlineDataPoint();
					}
				})
				.filterNot(
						(Predicate<String, PegelOnlineDataPoint>)(k,v) -> {
							// Discard all records that do not have a timestamp associated with it
							// As they cannot be used in calculations
							return v.getRecordTime() == null;
						}
				);

		formattedStream.to("PegelOnlineData",
				Produced.with(
						Serdes.String(),
						Serdes.serdeFrom(new DataPointSerializer<PegelOnlineDataPoint>(), new PegelOnlineDataPointDeserializer()))
				);
	}

}
