package com.example.pipeline.utils;

public class CommonUtils {

    public String getTableName(String tablePath) {
        return tablePath
            .replace("data/raw/", "")
            .replace(".parquet", "");
    }
}
