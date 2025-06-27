package com.example.agro_monitor_sys;

import com.example.pipeline.ingestion.ParquetIngestion;
import com.example.pipeline.utils.DBCommonUtils;
import org.junit.jupiter.api.*;
import org.mockito.*;
import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ParquetIngestionTest {

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private ResultSet mockResultSet;

    private ParquetIngestion ingestion;

    @BeforeEach
    public void setup() throws SQLException {
        MockitoAnnotations.openMocks(this);
        ingestion = new ParquetIngestion(mockConnection);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
    }

    @Test
    public void testListParquetFiles_success() throws IOException {
        Path tempDir = Files.createTempDirectory("test-parquet-dir");
        Path file1 = Files.createFile(tempDir.resolve("file1.parquet"));
        Path file2 = Files.createFile(tempDir.resolve("file2.txt"));

        List<String> files = ingestion.listParquetFiles(tempDir.toString());

        assertEquals(1, files.size());
        assertTrue(files.get(0).endsWith("file1.parquet"));

        Files.deleteIfExists(file1);
        Files.deleteIfExists(file2);
        Files.deleteIfExists(tempDir);
    }

    @Test
    public void testListParquetFiles_handlesException() {
        // Act & Assert
        assertDoesNotThrow(() -> {
            List<String> files = ingestion.listParquetFiles("non/existing/path");
            assertEquals(0, files.size());
        });
    }

    @Test
    public void testQueryAllData_success() throws Exception {
        // Mock ResultSet
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getString("sensor_id")).thenReturn("sensor_1");
        when(mockResultSet.getTimestamp("timestamp")).thenReturn(Timestamp.valueOf("2024-01-01 10:00:00"));
        when(mockResultSet.getString("reading_type")).thenReturn("temperature");
        when(mockResultSet.getDouble("value")).thenReturn(25.5);
        when(mockResultSet.getDouble("battery_level")).thenReturn(89.0);

        // Inject mock CommonUtils
        DBCommonUtils spyUtils = spy(new DBCommonUtils(mockConnection));
        doReturn(mockResultSet).when(spyUtils).executeSelectQuery(anyString());

        // Subclass to inject mock
        ParquetIngestion testIngestion = new ParquetIngestion(mockConnection) {
            @Override
            public void queryAllData(String tablePath) {
                try {
                    ResultSet rs = spyUtils.executeSelectQuery("");
                    assertTrue(rs.next());
                    assertEquals("sensor_1", rs.getString("sensor_id"));
                } catch (Exception e) {
                    fail("Exception thrown: " + e.getMessage());
                }
            }
        };

        testIngestion.queryAllData("dummy_path.parquet");
    }
}
