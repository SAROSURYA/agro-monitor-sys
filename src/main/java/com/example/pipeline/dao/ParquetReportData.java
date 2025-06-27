package com.example.pipeline.dao;

public class ParquetReportData {
    private String sensorId;
    private String updatedValue;
    private String updatedTimestamp;
    private String anomalousReading;

    public ParquetReportData(Builder builder) {
        this.sensorId = builder.sensorId;
        this.updatedValue = builder.updatedValue;
        this.updatedTimestamp = builder.updatedTimestamp;
        this.anomalousReading = builder.anomalousReading;
    }

    public String getSensorId() {
        return sensorId;
    }

    public String getUpdatedValue() {
        return updatedValue;
    }

    public String getUpdatedTimestamp() {
        return updatedTimestamp;
    }

    public String getAnomalousReading() {
        return anomalousReading;
    }

    @Override
    public String toString() {
        return "ParquetReportData{" +
                "sensorId='" + sensorId + '\'' +
                ", updatedValue='" + updatedValue + '\'' +
                ", updatedTimestamp='" + updatedTimestamp + '\'' +
                ", anomalousReading='" + anomalousReading + '\'' +
                '}';
    }

    public static class Builder {

        private String sensorId;
        private String updatedValue;
        private String updatedTimestamp;
        private String anomalousReading;

        public Builder sensorId(String sensorId) {
            this.sensorId = sensorId;
            return this;
        }

        public Builder updatedValue(String updatedValue) {
            this.updatedValue = updatedValue;
            return this;
        }

        public Builder updatedTimestamp(String updatedTimestamp) {
            this.updatedTimestamp = updatedTimestamp;
            return this;
        }

        public Builder anomalousReading(String anomalousReading) {
            this.anomalousReading = anomalousReading;
            return this;
        }

        public ParquetReportData build() {
            return new ParquetReportData(this);
        }
    }

}
