package com.example.pipeline;

import com.example.pipeline.dao.*;
import com.example.pipeline.ingestion.ParquetIngestion;
import com.example.pipeline.loading.ParquetWriter;
import com.example.pipeline.transformation.DataTransformation;
import com.example.pipeline.utils.CommonUtils;
import com.example.pipeline.validation.DataValidator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.IOException;
import java.sql.*;
import java.util.*;

@SpringBootApplication
public class AgroMonitorSysApplication {

	public static void main(String[] args) throws IOException, SQLException {

		String raw = "data/raw";
		String processed = "data/processed";

		String missingRecordsFileName = "missing_records";
		String emptyRecordsFileName = "empty_records";
		String reportFileName = "data_quality_report";
		String ingestionStatisticsFileName = "ingestion_statistics_records";

		Connection conn = DriverManager.getConnection("jdbc:duckdb:");

		ParquetIngestion ingestion = new ParquetIngestion(conn);
		DataValidator validator = new DataValidator(conn);
		DataTransformation transformation = new DataTransformation(conn);
		ParquetWriter writer = new ParquetWriter();
		CommonUtils CommonUtils = new CommonUtils();

		List<IngestionStatistics> statisticsList = new ArrayList<>();
		List<ParquetMissingReportData> validatorResultList = new ArrayList<>();

		List<String> parquet = ingestion.listParquetFiles(raw);
		parquet.forEach(file -> {

			IngestionStatistics statistics = new IngestionStatistics();
			String tableName = CommonUtils.getTableName(file);
			statistics.setFileName(tableName);

			boolean isCorrupt = ingestion.loadFile(file);
			if(!isCorrupt) {

				statistics.setIsCorrupt("No");

				ParquetMissingReportData MissingReportResult = validator.getTotalAndMissingValueCount(file);
				List<ParquetReportData> reportData = transformation.applyTransformations(raw, file);
				List<ParquetData> emptyParquetData = validator.getMissingValueRecords(file);

				if (MissingReportResult != null) {
					validatorResultList.add(MissingReportResult);
					statistics.setMissingValuesReport("Yes");
				} else {
					statistics.setMissingValuesReport("No");
				}

				if (emptyParquetData.size() > 0) {
					writer.generateMissingRecordReportCSV(
						file,
						processed,
						emptyRecordsFileName,
						emptyParquetData
					);
					statistics.setMissingRecordReport("Yes");
				} else {
					statistics.setMissingRecordReport("No");
				}

				if (reportData.size() > 0) {
					writer.generateFinalReportCSV(
						file,
						processed,
						reportFileName,
						reportData
					);
					statistics.setFinalReport("Yes");
				} else {
					statistics.setFinalReport("No");
				}

			} else {
				statistics.setIsCorrupt("Yes");
				statistics.setMissingValuesReport("No");
				statistics.setMissingRecordReport("No");
				statistics.setFinalReport("No");
			}

			statisticsList.add(statistics);
		});

		if(validatorResultList.size() > 0) {
			writer.generateMissingValuesReportCSV(
				processed,
				missingRecordsFileName,
				validatorResultList
			);
		}

		writer.generateIngestionStatisticsReportCSV(
				processed,
				ingestionStatisticsFileName,
				statisticsList
		);

		conn.close();
		SpringApplication.run(AgroMonitorSysApplication.class, args);
	}
}
