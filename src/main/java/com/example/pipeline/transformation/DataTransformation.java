package com.example.pipeline.transformation;

import com.example.pipeline.dao.ParquetReportData;
import com.example.pipeline.mapper.ParquetMapper;
import com.example.pipeline.utils.CommonUtils;
import com.example.pipeline.utils.DBCommonUtils;
import org.slf4j.*;
import java.sql.*;
import java.util.*;

public class DataTransformation {

    private static final Logger logger = LoggerFactory.getLogger(DataTransformation.class);

    private final Connection conn;

    public DataTransformation(Connection conn) {
        this.conn = conn;
    }

    public DBCommonUtils createDBUtils(Connection conn) {
        return new DBCommonUtils(conn);
    }

    public List<ParquetReportData> applyTransformations(String path, String tablePath) {

        DBCommonUtils commonUtils = createDBUtils(conn);
        try {
            String reportCreateQuery = this.reportCreateQuery(path, tablePath);
            commonUtils.executeUpdateQuery(reportCreateQuery);
        } catch(Exception e) {
            logger.error(
                "Failed to create a report parquet file " + tablePath,
                e.getMessage()
            );
        }

        List<ParquetReportData> reportDataList = new ArrayList<>();
        try {
            ParquetMapper pm = new ParquetMapper();

            String resultQuery = this.getResultQuery(tablePath);
            ResultSet rs = commonUtils.executeSelectQuery(resultQuery);
            while (rs.next()) {
                reportDataList.add(pm.parquetReportData(
                    rs.getString("sensor_id"),
                    rs.getString("updated_timestamp"),
                    rs.getString("updated_value"),
                    rs.getString("anomalous_reading")
                ));
            }

            logger.info(
                "Ingested {}: Final Report Data = {},",
                tablePath,
                reportDataList
            );
        } catch (Exception e) {
            logger.error(
                "Failed to execute the report query " + tablePath,
                e.getMessage()
            );
        }
        return reportDataList;
    }

    public String getResultQuery(String tablePath) {
        return String.format(
            "SELECT DISTINCT sensor_id, " +
            "CASE " +
            "WHEN reading_type = 'temperature' THEN value * 1.1 + 0.5 " +
            "WHEN reading_type = 'humidity' THEN value * 0.95 + 1.0 " +
            "ELSE value END AS updated_value, " +
            "STRFTIME(timestamp, '%%Y-%%m-%%dT%%H:%%M:%%S') || '+05:30' AS updated_timestamp, " +
            "CASE " +
            "WHEN reading_type = 'temperature' AND (value < 10 OR value > 40) THEN true " +
            "WHEN reading_type = 'humidity' AND (value < 20 OR value > 90) THEN true " +
            "ELSE false END AS anomalous_reading " +
            "FROM '%s' " +
            "where sensor_id IS NOT NULL " +
            "AND reading_type IS NOT NUll " +
            "AND timestamp IS NOT NULL " +
            "AND reading_type IS NOT NULL " +
            "AND value IS NOT NULL;", tablePath
        );
    }

    public String reportCreateQuery(String path, String tablePath) {
        CommonUtils CommonUtils = new CommonUtils();
        String tableName = CommonUtils.getTableName(tablePath);

        String resultQuery = this.getResultQuery(tablePath)
            .replace(";", "");

        String reportCreateQueryPath = path + "/report_" + tableName + ".parquet";

        return String.format(
            "COPY (%s) TO '%s' (FORMAT 'parquet', COMPRESSION 'snappy')",
            resultQuery,
            reportCreateQueryPath
        );
    }
}
