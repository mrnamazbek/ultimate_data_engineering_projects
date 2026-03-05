package com.namazbek.dataeng.models;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * EventMetrics model for Flink streaming analytics results
 */
public class EventMetrics {
    private Integer userId;
    private String metricType;
    private Long eventCount;
    private Timestamp windowStart;
    private Timestamp windowEnd;
    private Double avgProcessingTime;
    private String additionalData;

    public EventMetrics() {}

    public EventMetrics(Integer userId, String metricType, Long eventCount, 
                       Timestamp windowStart, Timestamp windowEnd, Double avgProcessingTime) {
        this.userId = userId;
        this.metricType = metricType;
        this.eventCount = eventCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.avgProcessingTime = avgProcessingTime;
    }

    // Getters and Setters
    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public Long getEventCount() {
        return eventCount;
    }

    public void setEventCount(Long eventCount) {
        this.eventCount = eventCount;
    }

    public Timestamp getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Timestamp windowStart) {
        this.windowStart = windowStart;
    }

    public Timestamp getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Timestamp windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Double getAvgProcessingTime() {
        return avgProcessingTime;
    }

    public void setAvgProcessingTime(Double avgProcessingTime) {
        this.avgProcessingTime = avgProcessingTime;
    }

    public String getAdditionalData() {
        return additionalData;
    }

    public void setAdditionalData(String additionalData) {
        this.additionalData = additionalData;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventMetrics that = (EventMetrics) o;
        return Objects.equals(userId, that.userId) &&
               Objects.equals(metricType, that.metricType) &&
               Objects.equals(eventCount, that.eventCount) &&
               Objects.equals(windowStart, that.windowStart) &&
               Objects.equals(windowEnd, that.windowEnd) &&
               Objects.equals(avgProcessingTime, that.avgProcessingTime) &&
               Objects.equals(additionalData, that.additionalData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, metricType, eventCount, windowStart, windowEnd, avgProcessingTime, additionalData);
    }

    @Override
    public String toString() {
        return "EventMetrics{" +
                "userId=" + userId +
                ", metricType='" + metricType + '\'' +
                ", eventCount=" + eventCount +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", avgProcessingTime=" + avgProcessingTime +
                ", additionalData='" + additionalData + '\'' +
                '}';
    }
}
