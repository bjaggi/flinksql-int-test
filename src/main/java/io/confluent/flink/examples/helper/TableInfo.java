package io.confluent.flink.examples.helper;

import java.util.List;

public final class TableInfo {
    private final String tableName;
    private final List<String> columnNames;
    private final List<String> dataTypes;
    private final List<String[]> data;

    public TableInfo(String tableName, List<String> columnNames, List<String> dataTypes, List<String[]> data) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
        this.data = data;
    }

    public String getTableName() { return tableName; }
    public List<String> getColumnNames() { return columnNames; }
    public List<String> getDataTypes() { return dataTypes; }
    public List<String[]> getData() { return data; }
} 