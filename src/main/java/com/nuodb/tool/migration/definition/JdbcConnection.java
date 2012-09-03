package com.nuodb.tool.migration.definition;

import java.util.HashMap;
import java.util.Map;

public class JdbcConnection extends JdbcConnectionSettings {
    private String driver;
    private String url;
    private String username;
    private String password;
    private Map<String, String> properties = new HashMap<String, String>();

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addProperty(String property, String value) {
        properties.put(property, value);
    }
}
