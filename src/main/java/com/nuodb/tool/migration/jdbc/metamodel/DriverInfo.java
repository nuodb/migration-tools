package com.nuodb.tool.migration.jdbc.metamodel;

public class DriverInfo {
    private String name;
    private String version;
    private String minorVersion;
    private String majorVersion;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getMinorVersion() {
        return minorVersion;
    }

    public void setMinorVersion(String minorVersion) {
        this.minorVersion = minorVersion;
    }

    public String getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(String majorVersion) {
        this.majorVersion = majorVersion;
    }

    @Override
    public String toString() {
        return String.format("name=%s, version=%s, minor version=%s, major version=%s",
                name, version, minorVersion, majorVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DriverInfo that = (DriverInfo) o;

        if (majorVersion != null ? !majorVersion.equals(that.majorVersion) : that.majorVersion != null) return false;
        if (minorVersion != null ? !minorVersion.equals(that.minorVersion) : that.minorVersion != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (minorVersion != null ? minorVersion.hashCode() : 0);
        result = 31 * result + (majorVersion != null ? majorVersion.hashCode() : 0);
        return result;
    }
}
