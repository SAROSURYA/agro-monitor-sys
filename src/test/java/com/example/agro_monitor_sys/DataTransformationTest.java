package com.example.agro_monitor_sys;

import com.example.pipeline.dao.ParquetReportData;
import com.example.pipeline.transformation.DataTransformation;
import com.example.pipeline.utils.DBCommonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class DataTransformationTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private DBCommonUtils mockDbUtils;

    @Mock
    private ResultSet mockResultSet;

    @InjectMocks
    private DataTransformation spyTransformation;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        spyTransformation = Mockito.spy(new DataTransformation(mockConnection));
        doReturn(mockDbUtils).when(spyTransformation).createDBUtils(any());
    }

    @Test
    public void testApplyTransformations_success() throws Exception {
        String tablePath = "data/raw/sensor_data.parquet";
        String path = "data/raw";

        when(mockDbUtils.executeSelectQuery(anyString())).thenReturn(mockResultSet);

        when(mockResultSet.next()).thenReturn(true, false); // only one row
        when(mockResultSet.getString("sensor_id")).thenReturn("S1");
        when(mockResultSet.getString("updated_value")).thenReturn("22.5");
        when(mockResultSet.getString("updated_timestamp")).thenReturn("2023-05-20T10:00:00+05:30");
        when(mockResultSet.getString("anomalous_reading")).thenReturn("false");

        // Mock COPY query execution
        doNothing().when(mockDbUtils).executeUpdateQuery(anyString());

        List<ParquetReportData> result = spyTransformation.applyTransformations(path, tablePath);

        assertEquals(1, result.size());
        ParquetReportData data = result.get(0);
        assertEquals("S1", data.getSensorId());
        assertEquals("22.5", data.getUpdatedValue());
        assertEquals("2023-05-20T10:00:00+05:30", data.getUpdatedTimestamp());
        assertEquals("false", data.getAnomalousReading());

        verify(mockDbUtils, times(1)).executeUpdateQuery(anyString());
        verify(mockDbUtils, times(1)).executeSelectQuery(anyString());
    }

    @Test
    public void testGetResultQuery_returnsExpectedQuery() {
        String tablePath = "data/raw/sensor_data.parquet";
        String query = spyTransformation.getResultQuery(tablePath);
        assertTrue(query.contains("FROM 'data/raw/sensor_data.parquet'"));
        assertTrue(query.contains("updated_value"));
        assertTrue(query.contains("anomalous_reading"));
    }

    @Test
    public void testReportCreateQuery_containsCorrectOutputPath() {
        String tablePath = "data/raw/sensor_data.parquet";
        String path = "data";

        String query = spyTransformation.reportCreateQuery(path, tablePath);

        assertTrue(query.contains("COPY (SELECT DISTINCT"));
        assertTrue(query.contains("TO 'data/report_sensor_data.parquet'"));
        assertTrue(query.contains("(FORMAT 'parquet'"));
    }
}
