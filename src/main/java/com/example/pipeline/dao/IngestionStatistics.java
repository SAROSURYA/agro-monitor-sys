package com.example.pipeline.dao;

public class IngestionStatistics {

    private String fileName;

    private String isCorrupt;

    private String missingRecordReport;

    private String finalReport;

    private String missingValuesReport;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getIsCorrupt() {
        return isCorrupt;
    }

    public void setIsCorrupt(String isCorrupt) {
        this.isCorrupt = isCorrupt;
    }

    public String getMissingRecordReport() {
        return missingRecordReport;
    }

    public void setMissingRecordReport(String missingRecordReport) {
        this.missingRecordReport = missingRecordReport;
    }

    public String getFinalReport() {
        return finalReport;
    }

    public void setFinalReport(String finalReport) {
        this.finalReport = finalReport;
    }

    public String getMissingValuesReport() {
        return missingValuesReport;
    }

    public void setMissingValuesReport(String missingValuesReport) {
        this.missingValuesReport = missingValuesReport;
    }

    @Override
    public String toString() {
        return "IngestionStatistics{" +
                "fileName='" + fileName + '\'' +
                ", isCorrupt='" + isCorrupt + '\'' +
                ", missingRecordReport='" + missingRecordReport + '\'' +
                ", finalReport='" + finalReport + '\'' +
                ", missingValuesReport='" + missingValuesReport + '\'' +
                '}';
    }
}
