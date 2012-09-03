package com.nuodb.tool.migration.jdbc.metamodel;

public class ColumnType {
    /**
     * SQL type from java.sql.Types
     */
    private final int dataType;
    /**
     * Data source dependent type name
     */
    private final String typeName;

    public ColumnType(int dataType, String typeName) {
        this.dataType = dataType;
        this.typeName = typeName;
    }

    public int getDataType() {
        return dataType;
    }

    public String getTypeName() {
        return typeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnType that = (ColumnType) o;

        if (dataType != that.dataType) return false;
        if (typeName != null ? !typeName.equals(that.typeName) : that.typeName != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = dataType;
        result = 31 * result + (typeName != null ? typeName.hashCode() : 0);
        return result;
    }
}
