package com.example.pipeline.dao;

public class ParquetMissingReportData {

    private String tableName;
    private String totalCount;
    private String sensorIdMissingCount;
    private String timestampMissingCount;
    private String readingTypeMissingCount;
    private String valueMissingCount;
    private String batteryLevelMissingCount;

    public ParquetMissingReportData(Builder builder) {
        this.tableName = builder.tableName;
        this.totalCount = builder.totalCount;
        this.sensorIdMissingCount = builder.sensorIdMissingCount;
        this.timestampMissingCount = builder.timestampMissingCount;
        this.readingTypeMissingCount = builder.readingTypeMissingCount;
        this.valueMissingCount = builder.valueMissingCount;
        this.batteryLevelMissingCount = builder.batteryLevelMissingCount;
    }

    public String getTableName() {return tableName;}

    public String getTotalCount() {
        return totalCount;
    }

    public String getSensorIdMissingCount() {
        return sensorIdMissingCount;
    }

    public String getTimestampMissingCount() {
        return timestampMissingCount;
    }

    public String getReadingTypeMissingCount() {
        return readingTypeMissingCount;
    }

    public String getValueMissingCount() {
        return valueMissingCount;
    }

    public String getBatteryLevelMissingCount() {
        return batteryLevelMissingCount;
    }

    @Override
    public String toString() {
        return "ParquetReportData{" +
                "totalCount='" + totalCount + '\'' +
                ", sensorIdCount='" + sensorIdMissingCount + '\'' +
                ", timestampCount='" + timestampMissingCount + '\'' +
                ", readingTypeCount='" + readingTypeMissingCount + '\'' +
                ", valueCount='" + valueMissingCount + '\'' +
                ", batteryLevelCount='" + batteryLevelMissingCount + '\'' +
                '}';
    }

    public static class Builder {

        private String tableName;
        private String totalCount;
        private String sensorIdMissingCount;
        private String timestampMissingCount;
        private String readingTypeMissingCount;
        private String valueMissingCount;
        private String batteryLevelMissingCount;

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder totalCount(String totalCount) {
            this.totalCount = totalCount;
            return this;
        }

        public Builder sensorIdMissingCount(String sensorIdMissingCount) {
            this.sensorIdMissingCount = sensorIdMissingCount;
            return this;
        }

        public Builder timestampMissingCount(String timestampMissingCount) {
            this.timestampMissingCount = timestampMissingCount;
            return this;
        }

        public Builder readingTypeMissingCount(String readingTypeMissingCount) {
            this.readingTypeMissingCount = readingTypeMissingCount;
            return this;
        }

        public Builder valueMissingCount(String valueMissingCount) {
            this.valueMissingCount = valueMissingCount;
            return this;
        }

        public Builder batteryLevelMissingCount(String batteryLevelMissingCount) {
            this.batteryLevelMissingCount = batteryLevelMissingCount;
            return this;
        }

        public ParquetMissingReportData build() {
            return new ParquetMissingReportData(this);
        }
    }

}
