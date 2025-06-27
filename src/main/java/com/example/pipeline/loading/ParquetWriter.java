package com.example.pipeline.loading;

import com.example.pipeline.dao.IngestionStatistics;
import com.example.pipeline.dao.ParquetData;
import com.example.pipeline.dao.ParquetMissingReportData;
import com.example.pipeline.dao.ParquetReportData;
import com.example.pipeline.utils.CommonUtils;
import org.slf4j.*;
import java.io.*;
import java.time.LocalDate;
import java.util.List;

public class ParquetWriter {

    private static final Logger logger = LoggerFactory.getLogger(ParquetWriter.class);

    public void generateMissingValuesReportCSV(
        String reportPath,
        String fileName,
        List<ParquetMissingReportData> validatorResultList
    ) throws IOException {

        try {
            fileName = reportPath + "/" + fileName + "_" + LocalDate.now() + ".csv" ;
            FileWriter csv = new FileWriter(fileName);

            csv.write(
            "Table Name," +
                "Total Count," +
                "Sensor Id Missing Count," +
                "Timestamp Missing Count," +
                "Reading Type Missing Count," +
                "Value Missing Count," +
                "Battery Level Missing Count \n"
            );

            for (ParquetMissingReportData resultList : validatorResultList) {

                String tableName = resultList
                    .getTableName()
                    .replace("data/raw/", "");

                csv.write(
                tableName + "," +
                    resultList.getTotalCount() + "," +
                    resultList.getSensorIdMissingCount() + "," +
                    resultList.getTimestampMissingCount() + "," +
                    resultList.getReadingTypeMissingCount() + "," +
                    resultList.getValueMissingCount() + "," +
                    resultList.getBatteryLevelMissingCount() + "\n"
                );
            }

            csv.close();
        } catch (Exception e) {
            logger.error("Failed to generate missing values report CSV ", e);
        }
    }

    public void generateMissingRecordReportCSV(
        String tablePath,
        String reportPath,
        String fileName,
        List<ParquetData> parquetData
    ) {

        try {
            String generateFileName = this.generateFileNameWithPath(
                tablePath,
                reportPath,
                fileName
            );
            FileWriter csv = new FileWriter(generateFileName);

            csv.write(
            "Sensor Id," +
                "Timestamp," +
                "Reading," +
                "Value," +
                "Battery Level \n"
            );

            for (ParquetData reportData : parquetData) {
                csv.write(
                reportData.getSensorId() + "," +
                    reportData.getTimestamp() + "," +
                    reportData.getReadingType() + "," +
                    reportData.getValue() + "," +
                    reportData.getBatteryLevel() + "\n"
                );
            }
            csv.close();
        } catch(Exception e) {
            logger.error("Failed to generate missing record report CSV ", e);
        }
    }

    public void generateFinalReportCSV(
        String tablePath,
        String reportPath,
        String fileName,
        List<ParquetReportData> parquetReportData
    ) {

        try {
            String generateFileName = this.generateFileNameWithPath(
                tablePath,
                reportPath,
                fileName
            );
            FileWriter csv = new FileWriter(generateFileName);

            csv.write(
            "Sensor Id," +
                "Updated Timestamp," +
                "Updated Value," +
                "Anomalous Reading \n"
            );

            for (ParquetReportData reportData : parquetReportData) {
                csv.write(
                reportData.getSensorId() + "," +
                    reportData.getUpdatedTimestamp() + "," +
                    reportData.getUpdatedValue() + "," +
                    reportData.getAnomalousReading() + "\n"
                );
            }

            csv.close();

        } catch(Exception e) {
            logger.error("Failed to generate final report CSV ", e);
        }
    }

    public void generateIngestionStatisticsReportCSV(
            String reportPath,
            String fileName,
            List<IngestionStatistics> statisticsList
    ) {
        try {
            fileName = reportPath + "/" + fileName + "_" + LocalDate.now() + ".csv";
            FileWriter csv = new FileWriter(fileName);

            csv.write(
            "File Name, " +
                "Is Corrupt, " +
                "Missing Record Report, " +
                "Final Report, " +
                "Missing Values Report\n"
            );

            for (IngestionStatistics statistics : statisticsList) {
                csv.write(
                statistics.getFileName() + "," +
                    statistics.getIsCorrupt() + "," +
                    statistics.getMissingRecordReport() + "," +
                    statistics.getFinalReport() + "," +
                    statistics.getMissingValuesReport()+ "\n"
                );
            }

            csv.close();

        }
        catch (Exception e) {
            logger.error("Failed to ingestion statistics report CSV ", e);
        }
    }

    public String generateFileNameWithPath(
        String tablePath,
        String reportPath,
        String fileName
    ) {
        CommonUtils CommonUtils = new CommonUtils();
        String tableName = CommonUtils.getTableName(tablePath);
        return reportPath + "/" + tableName + "_" + fileName + "_" + LocalDate.now() + ".csv";
    }
}
