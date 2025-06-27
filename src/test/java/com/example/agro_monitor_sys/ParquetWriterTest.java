package com.example.agro_monitor_sys;

import com.example.pipeline.dao.IngestionStatistics;
import com.example.pipeline.dao.ParquetData;
import com.example.pipeline.dao.ParquetMissingReportData;
import com.example.pipeline.dao.ParquetReportData;
import com.example.pipeline.loading.ParquetWriter;
import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ParquetWriterTest {

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
    private ParquetWriter parquetWriter;

    @BeforeEach
    public void setUp() {
        parquetWriter = new ParquetWriter();
    }

    @Test
    public void testGenerateMissingValuesReportCSV() throws IOException {
        List<ParquetMissingReportData> mockData = List.of(
            new ParquetMissingReportData.Builder()
                .tableName("data/raw/sensor1.parquet")
                .totalCount("100")
                .sensorIdMissingCount("2")
                .timestampMissingCount("1")
                .readingTypeMissingCount("0")
                .valueMissingCount("5")
                .batteryLevelMissingCount("3")
                .build()
        );

        String filename = "missing_values";
        parquetWriter.generateMissingValuesReportCSV(TEMP_DIR, filename, mockData);

        String expectedFilePath = TEMP_DIR + "/" + filename + "_" + LocalDate.now() + ".csv";
        File reportFile = new File(expectedFilePath);
        assertTrue(reportFile.exists());

        List<String> lines = Files.readAllLines(reportFile.toPath());
        assertEquals(2, lines.size()); // header + 1 row
        assertTrue(lines.get(1).contains("sensor1.parquet"));
    }

    @Test
    public void testGenerateMissingRecordReportCSV() throws IOException {
        List<ParquetData> mockData = List.of(
            new ParquetData.Builder()
                .sensorId("sensor-1")
                .timestamp(new Date())
                .readingType("temperature")
                .value(23.4)
                .batteryLevel(89.4)
                .build()
        );

        String path = "data/raw/sensor.parquet";
        String fileName = "missing_records";
        parquetWriter.generateMissingRecordReportCSV(path, TEMP_DIR, fileName, mockData);

        String expectedFileName = TEMP_DIR + "/sensor_missing_records_" + LocalDate.now() + ".csv";
        File reportFile = new File(expectedFileName);
        assertTrue(reportFile.exists());

        List<String> lines = Files.readAllLines(reportFile.toPath());
        assertEquals(2, lines.size());
        assertTrue(lines.get(1).contains("sensor-1"));
    }

    @Test
    public void testGenerateFinalReportCSV() throws IOException {
        List<ParquetReportData> mockData = List.of(
            new ParquetReportData.Builder()
                .sensorId("sensor-X")
                .updatedValue("25.4")
                .updatedTimestamp("2024-01-01T11:00:00+05:30")
                .anomalousReading("false")
                .build()
        );

        String path = "data/raw/final_table.parquet";
        String fileName = "final_report";
        parquetWriter.generateFinalReportCSV(path, TEMP_DIR, fileName, mockData);

        String expectedFileName = TEMP_DIR + "/final_table_final_report_" + LocalDate.now() + ".csv";
        File reportFile = new File(expectedFileName);
        assertTrue(reportFile.exists());

        List<String> lines = Files.readAllLines(reportFile.toPath());
        assertEquals(2, lines.size());
        assertTrue(lines.get(1).contains("sensor-X"));
    }

    @Test
    public void testGenerateIngestionStatisticsReportCSV() throws IOException {
        String fileName = "ingestion_stats";

        IngestionStatistics mockStats = new IngestionStatistics();
        mockStats.setFileName(fileName);
        mockStats.setIsCorrupt("No");
        mockStats.setMissingRecordReport("Yes");
        mockStats.setMissingValuesReport("Yes");
        mockStats.setFinalReport("Yes");

        List<IngestionStatistics> mockStatsList = new ArrayList<>();
        mockStatsList.add(mockStats);

        parquetWriter.generateIngestionStatisticsReportCSV(TEMP_DIR, fileName, mockStatsList);

        String expectedFileName = TEMP_DIR + "/" + fileName + "_" + LocalDate.now() + ".csv";
        File reportFile = new File(expectedFileName);
        assertTrue(reportFile.exists());

        List<String> lines = Files.readAllLines(reportFile.toPath());
        System.out.println(lines);
        assertEquals(2, lines.size());
        assertTrue(lines.get(1).contains("Yes"));
    }

}
