package com.namazbek.dataeng.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.namazbek.dataeng.models.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * Kafka deserializer for Event objects
 */
public class EventSchema implements DeserializationSchema<Event> {
    
    private static final Logger LOG = LoggerFactory.getLogger(EventSchema.class);
    private transient ObjectMapper objectMapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        
        try {
            ObjectNode json = objectMapper.readValue(message, ObjectNode.class);
            
            Event event = new Event();
            event.setId(json.get("id").asInt());
            event.setUserId(json.get("user_id").asInt());
            event.setValue(json.get("value").asText());
            
            // Parse timestamp - handle both ISO format and epoch
            String timestampStr = json.get("ts").asText();
            try {
                // Try parsing as ISO timestamp first
                Timestamp timestamp = Timestamp.valueOf(timestampStr.replace('T', ' ').replace('Z', ""));
                event.setTimestamp(timestamp);
            } catch (Exception e) {
                // Fallback to epoch milliseconds
                long epochMs = json.get("ts").asLong();
                event.setTimestamp(new Timestamp(epochMs));
            }
            
            return event;
            
        } catch (Exception e) {
            LOG.error("Failed to deserialize event: {}", new String(message), e);
            // Return null event to skip this message
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false; // Never end the stream
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
