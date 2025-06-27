package com.example.pipeline.ingestion;

import com.example.pipeline.dao.ParquetData;
import com.example.pipeline.mapper.ParquetMapper;
import com.example.pipeline.utils.DBCommonUtils;
import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import org.slf4j.*;

public class ParquetIngestion {

    private static final Logger logger = LoggerFactory.getLogger(ParquetIngestion.class);

    private final Connection conn;

    public ParquetIngestion(Connection conn) {
        this.conn = conn;
    }

    public List<String> listParquetFiles(String rawDir) throws IOException {
        List<String> list = new ArrayList<>();
        try {
            Stream<Path> paths = Files.list(Paths.get(rawDir));
            paths.filter(p -> p.toString().endsWith(".parquet"))
                    .forEach(p -> list.add(p.toString()));
        } catch(Exception e) {
            System.out.println("Parquet Ingestion Exception --> " + e.getMessage());
        }
        return list;
    }

    public boolean loadFile(String tablePath) {
        String query = String.format("SELECT * FROM '%s'", tablePath);
        boolean isCorrupt = true;
        try{
            DBCommonUtils commonUtils = new DBCommonUtils(conn);
            ResultSet rs = commonUtils.executeSelectQuery(query);
            isCorrupt = false;
        } catch (Exception e) {
            logger.error("Failed loading " + tablePath, e.getMessage());
        }
        return isCorrupt;
    }

    public void queryAllData(String tablePath) {
        String query = String.format("SELECT DISTINCT * FROM '%s'", tablePath);
        try {
            DBCommonUtils commonUtils = new DBCommonUtils(conn);
            ResultSet rs = commonUtils.executeSelectQuery(query);
            List<ParquetData> pdList = new ArrayList<>();

            while (rs.next()) {
                ParquetMapper pm = new ParquetMapper();

                pdList.add(pm.dataMapper(
                    rs.getString("sensor_id"),
                    rs.getTimestamp("timestamp"),
                    rs.getString("reading_type"),
                    rs.getDouble("value"),
                    rs.getDouble("battery_level")
                ));
            }
            logger.info("Ingested {}: All Data={}", tablePath, pdList);
        } catch (Exception e) {
            logger.error("Failed loading " + tablePath, e.getMessage());
        }
    }
}
