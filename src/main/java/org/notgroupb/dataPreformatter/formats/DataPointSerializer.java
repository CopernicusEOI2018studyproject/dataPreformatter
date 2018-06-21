package org.notgroupb.dataPreformatter.formats;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

// Taken from https://dzone.com/articles/kafka-sending-object-as-a-message
public class DataPointSerializer<T> implements Serializer<T> {

	@Override public void configure(Map map, boolean b) {}

	@Override public byte[] serialize(String arg0, T arg1) {
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();

		try {
			retVal = objectMapper.writeValueAsString(arg1).getBytes();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retVal;
	}

	@Override public void close() {}
}