package com.github.atokar.serializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer implements Deserializer<Map<String, Object>> {
    @Override
    public Map<String, Object> deserialize(String topic, byte[] data) {
        try {
            return new ObjectMapper().readValue(data, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }
}
