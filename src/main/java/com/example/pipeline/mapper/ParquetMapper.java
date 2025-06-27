package com.example.pipeline.mapper;

import com.example.pipeline.dao.ParquetData;
import com.example.pipeline.dao.ParquetMissingReportData;
import com.example.pipeline.dao.ParquetReportData;

import java.util.Date;

public class ParquetMapper {

    public ParquetData dataMapper(
            String sensor_id,
            Date timestamp,
            String reading_type,
            double value,
            double battery_level
    ) {

        return new ParquetData.Builder()
            .sensorId(sensor_id)
            .timestamp(timestamp)
            .readingType(reading_type)
            .value(value)
            .batteryLevel(battery_level)
            .build();
    }

    public ParquetMissingReportData parquetMissingReportData(
        String tableName,
        String totalCount,
        String sensorIdMissingCount,
        String timestampMissingCount,
        String readingTypeMissingCount,
        String valueMissingCount,
        String batteryLevelMissingCount
    ) {

        return new ParquetMissingReportData.Builder()
            .tableName(tableName)
            .totalCount(totalCount)
            .sensorIdMissingCount(sensorIdMissingCount)
            .timestampMissingCount(timestampMissingCount)
            .readingTypeMissingCount(readingTypeMissingCount)
            .valueMissingCount(valueMissingCount)
            .batteryLevelMissingCount(batteryLevelMissingCount)
            .build();
    }

    public ParquetReportData parquetReportData(
        String sensorId,
        String updatedTimestamp,
        String updatedValue,
        String anomalousReading
    ) {

        return new ParquetReportData.Builder()
            .sensorId(sensorId)
            .updatedTimestamp(updatedTimestamp)
            .updatedValue(updatedValue)
            .anomalousReading(anomalousReading)
            .build();
    }
}
