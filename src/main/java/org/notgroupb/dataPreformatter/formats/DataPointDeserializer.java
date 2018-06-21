package org.notgroupb.dataPreformatter.formats;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.core.GenericTypeResolver;

import com.fasterxml.jackson.databind.ObjectMapper;

// Taken from https://dzone.com/articles/kafka-sending-object-as-a-message
public class DataPointDeserializer<T> implements Deserializer<T> {
	
	@Override public void configure(Map<String, ?> arg0, boolean arg1) {}

	@SuppressWarnings("unchecked")
	@Override public T deserialize(String arg0, byte[] arg1) {
		ObjectMapper mapper = new ObjectMapper();
		T data = null;

		try {
			// Use Spring to get current generic class
			Class<T> genericType = (Class<T>) GenericTypeResolver.resolveTypeArgument(getClass(), DataPointDeserializer.class);
			data = mapper.readValue(arg1, genericType);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	@Override
	public void close() {}
}
