package com.namazbek.dataeng.models;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * Event model class matching the Kafka event schema
 */
public class Event {
    private Integer id;
    private Timestamp timestamp;
    private String value;
    private Integer userId;

    public Event() {}

    public Event(Integer id, Timestamp timestamp, String value, Integer userId) {
        this.id = id;
        this.timestamp = timestamp;
        this.value = value;
        this.userId = userId;
    }

    // Getters and Setters
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(id, event.id) &&
               Objects.equals(timestamp, event.timestamp) &&
               Objects.equals(value, event.value) &&
               Objects.equals(userId, event.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, value, userId);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", value='" + value + '\'' +
                ", userId=" + userId +
                '}';
    }
}
