package com.example.pipeline.validation;

import com.example.pipeline.dao.*;
import com.example.pipeline.mapper.ParquetMapper;
import com.example.pipeline.utils.DBCommonUtils;
import org.slf4j.*;
import java.sql.*;
import java.util.*;

public class DataValidator {

    private static final Logger logger = LoggerFactory.getLogger(DataValidator.class);

    private final Connection conn;

    public DataValidator(Connection conn) {
        this.conn = conn;
    }

    public ParquetMissingReportData getTotalAndMissingValueCount(String tablePath) {
        String query = String.format(
            "SELECT DISTINCT " +
            "'%s' AS table_name, COUNT(*) AS total_count, " +
            "SUM((sensor_id IS NULL)::INT) as sensor_id_missing_count, " +
            "SUM((timestamp IS NULL):: INT) as timestamp_missing_count, " +
            "SUM((reading_type IS NULL)::INT) as reading_type_missing_count, " +
            "SUM((value IS NULL)::INT) AS value_missing_count, " +
            "SUM((battery_level IS NULL):: INT) as battery_level_missing_count " +
            "FROM '%s';", tablePath, tablePath
        );

        ParquetMissingReportData mapResultData = null;
        try {
            DBCommonUtils commonUtils = new DBCommonUtils(conn);
            ResultSet rs = commonUtils.executeSelectQuery(query);

            ParquetMapper pm = new ParquetMapper();
            mapResultData = pm.parquetMissingReportData(
                tablePath,
                rs.getString("total_count"),
                rs.getString("sensor_id_missing_count"),
                rs.getString("timestamp_missing_count"),
                rs.getString("reading_type_missing_count"),
                rs.getString("value_missing_count"),
                rs.getString("battery_level_missing_count")
            );

            logger.info(
                "Ingested {}: Total And Missing Value Count = {},",
                tablePath,
                mapResultData
            );
        } catch (Exception e) {
            logger.error("Failed loading " + tablePath, e.getMessage());
        }
        return mapResultData;
    }

    public List<ParquetData> getMissingValueRecords(String tablePath) {
        String query = String.format(
            "SELECT DISTINCT sensor_id, timestamp, reading_type, " +
            "value, battery_level FROM '%s' where sensor_id IS NULL " +
            "OR reading_type IS NUll OR timestamp IS NULL " +
            "OR reading_type IS NULL OR value IS NULL;", tablePath
        );

        List<ParquetData> pdList = new ArrayList<>();
        try {
            DBCommonUtils commonUtils = new DBCommonUtils(conn);
            ResultSet rs = commonUtils.executeSelectQuery(query);

            ParquetMapper pm = new ParquetMapper();
            while (rs.next()) {
                pdList.add(pm.dataMapper(
                    rs.getString("sensor_id"),
                    rs.getTimestamp("timestamp"),
                    rs.getString("reading_type"),
                    rs.getDouble("value"),
                    rs.getDouble("battery_level")
                ));
            }

            logger.info(
                "Ingested {}: Missing Value Records = {},",
                tablePath,
                pdList
            );
        } catch(Exception e) {
            logger.error("Failed loading " + tablePath, e.getMessage());
        }
        return pdList;
    }
}
