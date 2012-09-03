package com.nuodb.tool.migration.jdbc.metamodel;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetMetaModel {

    private int columnCount;
    private List<String> columns;
    private List<Integer> columnTypes;

    public ResultSetMetaModel(ResultSetMetaData meta) throws SQLException {
        int columnCount = meta.getColumnCount();

        List<String> columns = new ArrayList<String>();
        List<Integer> columnTypes = new ArrayList<Integer>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(meta.getColumnLabel(i));
            columnTypes.add(meta.getColumnType(i));
        }
        this.columnCount = columnCount;
        this.columns = columns;
        this.columnTypes = columnTypes;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public boolean hasColumn(String column) {
        return columns.contains(column);
    }

    public List<Integer> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(List<Integer> columnTypes) {
        this.columnTypes = columnTypes;
    }
}
