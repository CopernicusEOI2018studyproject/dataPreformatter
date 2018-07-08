package org.notgroupb.dataPreformatter;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;

public class DataFormatterApplication {

	final static StreamsBuilder builder = new StreamsBuilder();;

	public static void main(String[] args) {
		//		DataFormatterApplication main = new DataFormatterApplication();
		try {
			HygonDataFormatter.preformatHygonStream(builder);
			HygonDataFormatter.formatHygonStream(builder);
			
			PegelOnlineDataFormatter.formatPegelOnlineStream(builder);
			
			DataDeduplicator.deduplicateData(builder);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		final KafkaStreams streams = new KafkaStreams(builder.build(), kStreamsConfigs());
		streams.start();
		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private static StreamsConfig kStreamsConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "HygonAggregator");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		return new StreamsConfig(props);
	}

	
}
