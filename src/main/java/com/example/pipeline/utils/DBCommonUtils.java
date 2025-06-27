package com.example.pipeline.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBCommonUtils {

    private final Connection conn;

    public DBCommonUtils(Connection conn) {
        this.conn = conn;
    }

    public ResultSet executeSelectQuery(String query) throws SQLException {
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(query);
        rs.next();
        return rs;
    }

    public void executeUpdateQuery(String query) throws SQLException {
        Statement st = conn.createStatement();
        st.executeUpdate(query);
    }
}
