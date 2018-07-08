package org.notgroupb.dataPreformatter;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.notgroupb.formats.HygonDataPoint;
import org.notgroupb.formats.PegelOnlineDataPoint;
import org.notgroupb.formats.deserialize.HygonDataPointDeserializer;
import org.notgroupb.formats.deserialize.PegelOnlineDataPointDeserializer;
import org.notgroupb.formats.serialize.DataPointSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Based on https://github.com/confluentinc/kafka-streams-examples/blob/4.0.0-post/src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java
 */
public class DataDeduplicator {
	
	final static Logger logger = LoggerFactory.getLogger(DataDeduplicator.class);
	
	private static String storeName = "ingested-store";
	
	/*
	 * Deduplicates Data. Data is discarded if it has been ingested during the last 48hours.
	 * Equality between ingested Data is determined by Stationname + Recordtime.
	 */
	static void deduplicateData(StreamsBuilder builder) {
		
		// How long ingestion is remembered
		long maintainDuration = TimeUnit.HOURS.toMillis(48);
		// How long ingestion is stored
		long retentionDuration = maintainDuration;
		
		// Input Queue Names
		final String inputHygon = "HygonDataRaw";
		final String inputPegelOnline = "PegelOnlineDataRaw";

		// Create Store to log already ingested DataPoints
		StoreBuilder<WindowStore<String, Long>> dedupStore = Stores.windowStoreBuilder(
				Stores.persistentWindowStore(storeName,
											 retentionDuration,
											 2,
											 maintainDuration,
											 false),
				Serdes.String(),
				Serdes.Long());
		builder.addStateStore(dedupStore);
		
		// Remove Duplicates from Hygon Data Stream
		KStream<String, HygonDataPoint> inputStreamHygon = builder.stream(inputHygon, Consumed.with(Serdes.String(), getHygonSerdes()));
		KStream<String, HygonDataPoint> outputStreamHygon = inputStreamHygon.transform(
					() -> new DedupTransformer<String, HygonDataPoint, String>(
							maintainDuration,
							(KeyValueMapper<String, HygonDataPoint, String>)(k, v) -> {
								return inputHygon + v.getName() + v.getRecordTime().toString();
							}),
					storeName);
		outputStreamHygon.to("HygonData",Produced.with(Serdes.String(),getHygonSerdes()));
		
		// Remove Duplicates from PegelOnline Data Stream
		KStream<String, PegelOnlineDataPoint> inputStreamPegelOnline = builder.stream(inputPegelOnline, Consumed.with(Serdes.String(), getPegelOnlineSerdes()));
		KStream<String, PegelOnlineDataPoint> outputStreamPegelOnline = inputStreamPegelOnline.transform(
					() -> new DedupTransformer<>(
							maintainDuration,
							(k, v) -> {
								return inputPegelOnline + v.getName() + v.getRecordTime().toString();
							}),
					storeName);
		outputStreamPegelOnline.to("PegelOnlineData", Produced.with(Serdes.String(), getPegelOnlineSerdes()));
	}

	// Deduplicates Data
	private static class DedupTransformer<K,V,E> implements Transformer<K,V,KeyValue<K,V>> {
		
		private ProcessorContext context;
		
		private WindowStore<E, Long> eventStore;
		
		private final long leftDuration;
		private final long rightDuration;
		
		private final KeyValueMapper<K,V,E> idExtractor;
		
		@SuppressWarnings("unchecked")
		@Override
		public void init(final ProcessorContext context) {
			this.context = context;
			eventStore = (WindowStore<E, Long>) context.getStateStore(storeName);
		}
		
		DedupTransformer(long duration, KeyValueMapper<K,V,E> idExtractor) {
			leftDuration = duration /2;
			rightDuration = duration - leftDuration;
			this.idExtractor = idExtractor;
		}
		
		public KeyValue<K,V> transform(final K key, final V value) {
			E eventId = idExtractor.apply(key, value);
			if (eventId == null) {
				return KeyValue.pair(key, value);
			} else {
				KeyValue<K,V> result;
				if (isDuplicate(eventId)) {
					result = null;
				} else {
					result = KeyValue.pair(key, value);
					eventStore.put(eventId, context.timestamp(), context.timestamp());
				}
				return result;
			}
		}
		
		private boolean isDuplicate(final E eventId) {
			long eventTime = context.timestamp();
			WindowStoreIterator<Long> timeIterator = eventStore.fetch(
					eventId,
					eventTime - leftDuration,
					eventTime + rightDuration);
			boolean isDuplicate = timeIterator.hasNext();
			timeIterator.close();
			return isDuplicate;
		}
		
	    @Override
	    public KeyValue<K, V> punctuate(final long timestamp) {
	      // our windowStore segments are closed automatically
	      return null;
	    }
	    
	    @Override
	    public void close() {}
	}
	
	private static Serde<PegelOnlineDataPoint> getPegelOnlineSerdes() {
		return Serdes.serdeFrom(new DataPointSerializer<PegelOnlineDataPoint>(), new PegelOnlineDataPointDeserializer());
	}
	
	private static Serde<HygonDataPoint> getHygonSerdes() {
		return Serdes.serdeFrom(new DataPointSerializer<HygonDataPoint>(), new HygonDataPointDeserializer());
	}	
}
