package com.example.pipeline.dao;

import java.util.Date;

public class ParquetData {
    public String sensorId;
    public Date timestamp;
    public String readingType;
    public double value;
    public double batteryLevel;

    public ParquetData(Builder builder) {
        this.sensorId = builder.sensorId;
        this.timestamp = builder.timestamp;
        this.readingType = builder.readingType;
        this.value = builder.value;
        this.batteryLevel = builder.batteryLevel;
    }

    public String getSensorId() {
        return sensorId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getReadingType() {
        return readingType;
    }

    public double getValue() {
        return value;
    }

    public double getBatteryLevel() {
        return batteryLevel;
    }

    @Override
    public String toString() {
        return "{" +
                "sensor_id='" + sensorId + '\'' +
                ", timestamp=" + timestamp +
                ", reading_type='" + readingType + '\'' +
                ", value=" + value +
                ", battery_level=" + batteryLevel +
                '}';
    }

    public static class Builder {

        public String sensorId;
        public Date timestamp;
        public String readingType;

        public double value;
        public double batteryLevel;

        public Builder sensorId(String sensorId) {
            this.sensorId = sensorId;
            return this;
        }
        public Builder timestamp(Date timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        public Builder readingType(String readingType) {
            this.readingType = readingType;
            return this;
        }
        public Builder value(double value) {
            this.value = value;
            return this;
        }
        public Builder batteryLevel(double batteryLevel) {
            this.batteryLevel = batteryLevel;
            return this;
        }

        public ParquetData build() {
            return new ParquetData(this);
        }
    }

}
