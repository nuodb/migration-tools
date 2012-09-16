package com.nuodb.tools.migration.jdbc.metamodel;

public class DatabaseInfo {
    private String productName;
    private String productVersion;
    private int minorVersion;
    private int majorVersion;

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductVersion() {
        return productVersion;
    }

    public void setProductVersion(String productVersion) {
        this.productVersion = productVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public void setMinorVersion(int minorVersion) {
        this.minorVersion = minorVersion;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(int majorVersion) {
        this.majorVersion = majorVersion;
    }

    @Override
    public String toString() {
        return String.format("product name=%s, product version=%s, minor version=%d, major version=%d",
                productName, productVersion, minorVersion, majorVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatabaseInfo that = (DatabaseInfo) o;

        if (majorVersion != that.majorVersion) return false;
        if (minorVersion != that.minorVersion) return false;
        if (productName != null ? !productName.equals(that.productName) : that.productName != null) return false;
        if (productVersion != null ? !productVersion.equals(that.productVersion) : that.productVersion != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = productName != null ? productName.hashCode() : 0;
        result = 31 * result + (productVersion != null ? productVersion.hashCode() : 0);
        result = 31 * result + minorVersion;
        result = 31 * result + majorVersion;
        return result;
    }
}
