package com.example.agro_monitor_sys;

import com.example.pipeline.dao.ParquetData;
import com.example.pipeline.dao.ParquetMissingReportData;
import com.example.pipeline.dao.ParquetReportData;
import com.example.pipeline.mapper.ParquetMapper;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class ParquetMapperTest {

    private final ParquetMapper mapper = new ParquetMapper();

    @Test
    public void testDataMapper() {

        Date timestamp = new Date();
        ParquetData result = mapper.dataMapper(
            "sensor_1",
            timestamp,
            "temperature",
            23.5,
            75.0
        );

        assertEquals("sensor_1", result.getSensorId());
        assertEquals(timestamp, result.getTimestamp());
        assertEquals("temperature", result.getReadingType());
        assertEquals(23.5, result.getValue(), 0.001);
        assertEquals(75.0, result.getBatteryLevel(), 0.001);
    }

    @Test
    public void testParquetMissingReportData() {
        ParquetMissingReportData result = mapper.parquetMissingReportData(
            "sensor_data",
            "1000",
            "5",
            "3",
            "2",
            "1",
            "0"
        );

        assertEquals("sensor_data", result.getTableName());
        assertEquals("1000", result.getTotalCount());
        assertEquals("5", result.getSensorIdMissingCount());
        assertEquals("3", result.getTimestampMissingCount());
        assertEquals("2", result.getReadingTypeMissingCount());
        assertEquals("1", result.getValueMissingCount());
        assertEquals("0", result.getBatteryLevelMissingCount());
    }

    @Test
    public void testParquetReportData() {
        ParquetReportData result = mapper.parquetReportData(
            "sensor_5",
            "2025-06-25T10:15:00+05:30",
            "25.8",
            "true"
        );

        assertEquals("sensor_5", result.getSensorId());
        assertEquals("2025-06-25T10:15:00+05:30", result.getUpdatedTimestamp());
        assertEquals("25.8", result.getUpdatedValue());
        assertEquals("true", result.getAnomalousReading());
    }
}

