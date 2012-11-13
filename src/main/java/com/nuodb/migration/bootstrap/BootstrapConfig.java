package com.nuodb.migration.bootstrap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

public class BootstrapConfig {

    public static final String HOME = "nuodb.migration.home";

    public static final String HOME_TOKEN = "${" + HOME + "}";

    public static final String CONFIG_PROPERTY = "bootstrap.config";

    public static final String CONFIG = "bootstrap.properties";

    public static final String CONFIG_FOLDER = "conf";

    private static Properties properties;

    private static final Log log = LogFactory.getLog(BootstrapConfig.class);

    static {
        setHome();
        loadConfig();
    }

    private static void setHome() {
        if (System.getProperty(HOME) != null) {
            return;
        }
        String home;
        try {
            System.setProperty(HOME, home = new File(System.getProperty("user.dir"), "..").getCanonicalPath());
        } catch (IOException e) {
            System.setProperty(HOME, home = System.getProperty("user.dir"));
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("System property %s is set to %s", HOME, home));
        }
    }

    public static String expand(String property) {
        int i;
        String before = property;
        boolean replace = false;
        while ((i = property.indexOf(HOME_TOKEN)) >= 0) {
            replace = true;
            if (i > 0) {
                property = property.substring(0, i) + getHome() + property.substring(i + HOME_TOKEN.length());
            } else {
                property = getHome() + property.substring(HOME_TOKEN.length());
            }
        }
        return property;
    }

    public static String getProperty(String name) {
        return expand(properties.getProperty(name));
    }

    public static String getProperty(String name, String defaultValue) {
        return expand(properties.getProperty(name, defaultValue));
    }

    public static String getHome() {
        return System.getProperty(HOME, System.getProperty("user.dir"));
    }

    private static void loadConfig() {
        Throwable error = null;
        InputStream is = getConfigFromParameter();
        if (is == null) {
            is = getConfigFromHome();
        }
        if (is == null) {
            is = getDefaultConfig();
        }
        if (is != null)
            try {
                properties = new Properties();
                properties.load(is);
                is.close();
            } catch (Throwable t) {
                error = t;
            }
        if (is == null || error != null) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to load bootstrap properties", error);
            }
            properties = new Properties();
        }
        Enumeration enumeration = properties.propertyNames();
        while (enumeration.hasMoreElements()) {
            String name = (String) enumeration.nextElement();
            String value = properties.getProperty(name);
            if (value != null) {
                System.setProperty(name, value);
            }
        }
    }

    private static InputStream getConfigFromParameter() {
        InputStream stream = null;
        try {
            String config = System.getProperty(CONFIG_PROPERTY);
            if (config != null) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Attempting to load properties from %s", config));
                }
                stream = (new URL(config)).openStream();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("No config file specified as an argument of system property %s", CONFIG_PROPERTY));
                }
            }
        } catch (Throwable t) {
            // ignored
        }
        return stream;
    }

    private static InputStream getConfigFromHome() {
        InputStream stream = null;
        File properties = null;
        try {
            File home = new File(getHome());
            properties = new File(new File(home, CONFIG_FOLDER), CONFIG);
            stream = new FileInputStream(properties);
        } catch (Throwable t) {
            // ignored
        }
        if (stream != null) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Bootstrap config loaded from %s", properties.getAbsolutePath()));
            }
        } else if (properties != null) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Bootstrap config can't be loaded from %s", properties.getAbsolutePath()));
            }
        }
        return stream;
    }

    public static InputStream getDefaultConfig() {
        InputStream stream = null;
        try {
            String config = CONFIG;
            stream = BootstrapConfig.class.getResourceAsStream(config);
            if (stream != null) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Default bootstrap config found at %s", config));
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Default bootstrap config can't be found at %s", config));
                }
            }
        } catch (Throwable t) {
            // ignored
        }
        return stream;
    }
}
