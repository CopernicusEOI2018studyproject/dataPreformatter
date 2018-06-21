package org.notgroupb.dataPreformatter;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.cts.CRSFactory;
import org.cts.crs.CoordinateReferenceSystem;
import org.cts.crs.GeodeticCRS;
import org.cts.op.CoordinateOperation;
import org.cts.op.CoordinateOperationFactory;
import org.cts.registry.EPSGRegistry;
import org.cts.registry.RegistryManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.notgroupb.dataPreformatter.formats.DataPointDeserializer;
import org.notgroupb.dataPreformatter.formats.DataPointSerializer;
import org.notgroupb.dataPreformatter.formats.HygonDataPoint;
import org.notgroupb.dataPreformatter.formats.PegelOnlineDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.util.StringUtils;


@Configuration
@EnableKafka
@EnableKafkaStreams
public class DataFormatter {

	@Autowired private KafkaProperties kafkaProperties;

	final static Logger logger = LoggerFactory.getLogger(DataFormatter.class);

	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
	public StreamsConfig kStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "HygonAggregator");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		return new StreamsConfig(props);
	}

	@Bean
	KStream <byte[], String> formatPegelOnlineStream(StreamsBuilder builder) {
		// Input Stream
		KStream<byte[], String> dataStream = builder.stream("PegelOnlineRAW");
		// Output Stream 
		KStream<String, PegelOnlineDataPoint> formattedStream = dataStream.flatMap(
				(KeyValueMapper<? super byte[], ? super String, ? extends Iterable<? extends KeyValue<String,PegelOnlineDataPoint>>>)
				(k ,v) -> {
					List<KeyValue<String, PegelOnlineDataPoint>> result = new ArrayList<>();
					JSONArray allStations;

					// Remove noise & Bit hacking cause it somehow gets broken in the process
					v = StringUtils.trimAllWhitespace(v);
					v = StringUtils.deleteAny(v, "\n").substring(7);

					// Convert to JSON Object and extract Data into custom Format
					try {
						allStations = new JSONArray(v);
						for (int i = 0; i < allStations.length();i++) {
							JSONObject element = allStations.getJSONObject(i);
							PegelOnlineDataPoint dp = new PegelOnlineDataPoint();
							String key = element.getString("longname");
							
							// Get Water Level Timeseries
							JSONObject waterleveltimeseries = null;
							JSONArray timeseries = element.getJSONArray("timeseries");
							for (int j = 0; j < timeseries.length(); j++) {
								String wlt = (String) timeseries.getJSONObject(j).get("shortname");
								if (wlt == "W") {
									waterleveltimeseries = timeseries.getJSONObject(j);
									break;
								}
							}
							// Discard Datapoint if no water level is measured
							if (waterleveltimeseries == null) {
								break;
							}
							
							// Get measurement time
							dp.setRecordTime(new Date((String)waterleveltimeseries.getJSONObject("currentMeasurement").get("timestamp")));
							// Get measurement
							dp.setMeasurement((float) waterleveltimeseries.getJSONObject("currentMeasurement").get("value"));
							
							// Get Characteristic Values
							JSONArray characteristicValues = waterleveltimeseries.getJSONArray("characteristicValues");
							for (int j = 0; j < characteristicValues.length(); j++) {
								JSONObject obj = characteristicValues.getJSONObject(j);
								switch((String)obj.get("shortname")) {
									case "HHW": {
										dp.setHhw((double) obj.get("value"));
										break;
									}
									case "MHW": {
										dp.setMhw((double) obj.get("value"));
										break;
									}
									case "MW": {
										dp.setMw((double) obj.get("value"));
										break;
									}
									case "MNW": {
										dp.setMnw((double) obj.get("value"));
										break;
									}
								}
							}
								
							// Get coordinates
							dp.setLon(element.getLong("longitude"));
							dp.setLat(element.getLong("latitude"));

							result.add(new KeyValue<>(key, dp));
						}
					} catch (JSONException e) {
						logger.error("Error parsing response to JSON. Error was: " + e);
					}
					return result;
				}
		);

		formattedStream.to("PegelOnlineData",
				Produced.with(
						Serdes.String(),
						Serdes.serdeFrom(new DataPointSerializer<PegelOnlineDataPoint>(), new DataPointDeserializer<PegelOnlineDataPoint>()))
				);
		return dataStream;
	}

	@Bean
	KStream <String,String> preformatHygonStream(StreamsBuilder builder) {
		// Input Data Stream
		// Aggregate Data points and only keep latest Measurement
		KTable<byte[], String> rawdataStream = builder.table("HygonWLRaw");
		
		// Preformats Data Stream to use Station Name as Key
		KStream<String, String> preformattedDataStream = rawdataStream.toStream().map(
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
		return preformattedStationsStream;
	}


	@Bean
	KStream <String, HygonDataPoint> formatHygonStream(StreamsBuilder builder) throws Exception { 

		// Reimport Stations as Table
		KTable<String,String> stationsTable = builder.table("PreformattedHygonStations", Consumed.with(Serdes.String(), Serdes.String()));
		KStream<String,String> hygonDataStream = builder.stream("PreformattedHygonData", Consumed.with(Serdes.String(), Serdes.String()));

		// Coordinate Library Setup
		CRSFactory cRSFactory = new CRSFactory();
		RegistryManager registryManager = cRSFactory.getRegistryManager();
		registryManager.addRegistry(new EPSGRegistry());

		CoordinateReferenceSystem utm32Ncrs = cRSFactory.getCRS("EPSG:32632");
		CoordinateReferenceSystem wgs84crs = cRSFactory.getCRS("EPSG:4326");
		Set<CoordinateOperation> transformations = CoordinateOperationFactory.createCoordinateOperations(
				(GeodeticCRS) utm32Ncrs,
				(GeodeticCRS) wgs84crs);

		KStream<String, HygonDataPoint> outputStream = hygonDataStream.join(stationsTable, 
				(ValueJoiner<String,String,HygonDataPoint>)(data, station) -> {
					try {
						String[] dataArray = data.split(";");
						String[] stationArray = station.split(";");

						HygonDataPoint dp = new HygonDataPoint();
						try {
							// Stations: Name;Gewaesser;MNW;MHW;Mittel;Informationsstufe 1;Informationsstufe 2;
							// Informationsstufe 3;Einzugsgebiet;east;north
							
							dp.setRecordTime(new Date(dataArray[1]));
							dp.setMeasurement(Float.parseFloat(dataArray[3]));
							
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

							if (transformations.size() != 0) {
								for (CoordinateOperation op : transformations) {
									// Transform coord using the op CoordinateOperation from crs1 to crs2
									double[] wgs84coords  = op.transform(new double[] {
											Float.parseFloat(stationArray[8]),
											Float.parseFloat(stationArray[9])});
									dp.setLon(wgs84coords[0]);
									dp.setLat(wgs84coords[1]);
									break;
								}
							} else {
								throw new Exception("Could not find any transformation from UTM32N to WGS84");
							}

						} catch (Exception e) {
							logger.error("Error parsing Data to HygonDataPoint. Exception was:" + e);
							return null;
						}
						return dp;
					} catch (Exception e) {
						logger.error("Error merging Preformatted HygonDataStreams. Exception was:" + e);
						return null;
					} 
				}
				);
		outputStream.to("HygonData",
				Produced.with(
						Serdes.String(),
						Serdes.serdeFrom(new DataPointSerializer<HygonDataPoint>(), new DataPointDeserializer<HygonDataPoint>()))
				);
		return outputStream;
	}

}
