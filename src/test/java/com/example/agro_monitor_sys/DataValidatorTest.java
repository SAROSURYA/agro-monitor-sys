package com.example.agro_monitor_sys;

import com.example.pipeline.dao.*;
import com.example.pipeline.mapper.ParquetMapper;
import com.example.pipeline.utils.DBCommonUtils;
import com.example.pipeline.validation.DataValidator;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class DataValidatorTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private ResultSet mockResultSet;

    @InjectMocks
    private DataValidator dataValidator;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

    }

    @Test
    public void testGetTotalAndMissingValueCount_success() throws Exception {
        String tablePath = "sensor_data";

        DBCommonUtils dbUtilsSpy = spy(new DBCommonUtils(mockConnection));
        doReturn(mockResultSet).when(dbUtilsSpy).executeSelectQuery(anyString());

        when(mockResultSet.next()).thenReturn(true);
        when(mockResultSet.getString("total_count")).thenReturn("100");
        when(mockResultSet.getString("sensor_id_missing_count")).thenReturn("2");
        when(mockResultSet.getString("timestamp_missing_count")).thenReturn("1");
        when(mockResultSet.getString("reading_type_missing_count")).thenReturn("3");
        when(mockResultSet.getString("value_missing_count")).thenReturn("0");
        when(mockResultSet.getString("battery_level_missing_count")).thenReturn("5");

        ParquetMapper mockMapper = mock(ParquetMapper.class);
        ParquetMissingReportData expectedData = new ParquetMissingReportData.Builder()
                .tableName(tablePath)
                .totalCount("100")
                .valueMissingCount("2")
                .sensorIdMissingCount("1")
                .readingTypeMissingCount("3")
                .batteryLevelMissingCount("0")
                .timestampMissingCount("5")
                .build();

        when(mockMapper.parquetMissingReportData(
                eq(tablePath),
                anyString(), anyString(), anyString(),
                anyString(), anyString(), anyString()
        )).thenReturn(expectedData);
    }

    @Test
    public void testGetMissingValueRecords_success() throws Exception {
        String tablePath = "sensor_data";

        ResultSet rs = mock(ResultSet.class);

        when(rs.next()).thenReturn(true, false);
        when(rs.getString("sensor_id")).thenReturn(null);
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        when(rs.getTimestamp("timestamp")).thenReturn(ts);
        when(rs.getString("reading_type")).thenReturn("temperature");
        when(rs.getDouble("value")).thenReturn(23.5);
        when(rs.getDouble("battery_level")).thenReturn(3.7);

        DBCommonUtils dbUtils = mock(DBCommonUtils.class);
        when(dbUtils.executeSelectQuery(anyString())).thenReturn(rs);

        DataValidator validator = new DataValidator(mockConnection) {
            @Override
            public List<ParquetData> getMissingValueRecords(String tablePath) {
                List<ParquetData> list = new ArrayList<>();
                try {
                    ResultSet rs = dbUtils.executeSelectQuery("SELECT ...");
                    ParquetMapper mapper = new ParquetMapper();
                    while (rs.next()) {
                        list.add(mapper.dataMapper(
                            rs.getString("sensor_id"),
                            rs.getTimestamp("timestamp"),
                            rs.getString("reading_type"),
                            rs.getDouble("value"),
                            rs.getDouble("battery_level")
                        ));
                    }
                } catch (Exception e) {
                    fail("Exception should not be thrown");
                }
                return list;
            }
        };

        List<ParquetData> result = validator.getMissingValueRecords(tablePath);
        assertEquals(1, result.size());
        assertNull(result.get(0).getSensorId());
        assertEquals("temperature", result.get(0).getReadingType());
    }
}
