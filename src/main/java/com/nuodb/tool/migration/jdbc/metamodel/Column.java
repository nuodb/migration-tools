package com.nuodb.tool.migration.jdbc.metamodel;

public class Column {
    /**
     * Default precision is maximum value
     */
    public static final int DEFAULT_PRECISION = 38;
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_RADIX = 10;

    private Table table;

    private Name name;

    private ColumnType type;
    /**
     * Holds column size.
     */
    private int size;
    /**
     * The maximum total number of decimal digits that can be stored, both to the left and to the right of the decimal
     * point. The precision is in the range of 1 through the maximum precision of 38.
     */
    private int precision = DEFAULT_PRECISION;
    /**
     * The number of fractional digits for numeric data types.
     */
    private int scale = DEFAULT_SCALE;
    /**
     * Contains column remarks, may be null.
     */
    private String comment;
    /**
     * Radix for numbers, typically 2 or 10.
     */
    private int radix = DEFAULT_RADIX;
    /**
     * Ordinal position of column in table, starting at 1.
     */
    private int position;
    /**
     * Determines the nullability for a column.
     */
    private boolean nullable;
    /**
     * Indicates whether this column is auto incremented.
     */
    private boolean autoIncrement;
    /**
     * Specifies whether column value is unique in the table.
     */
    private boolean unique;

    private String defaultValue;

    public Column(Table table, Name name) {
        this.table = table;
        this.name = name;
    }

    public Table getTable() {
        return table;
    }

    public Name getName() {
        return name;
    }

    public void setName(Name name) {
        this.name = name;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getRadix() {
        return radix;
    }

    public void setRadix(int radix) {
        this.radix = radix;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        return getName().toString();
    }
}
