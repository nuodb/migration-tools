package com.nuodb.migrator.bootstrap.config;

import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static com.nuodb.migrator.bootstrap.config.Config.*;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("unchecked")
public class PropertiesConfigLoader {

    private static final Logger logger = getLogger(PropertiesConfigLoader.class);

    private Replacer replacer;

    static {
        setHome();
    }

    private static void setHome() {
        if (getProperty(ROOT) != null) {
            return;
        }
        try {
            System.setProperty(ROOT, new File(getProperty("user.dir"), "..").getCanonicalPath());
        } catch (IOException e) {
            System.setProperty(ROOT, getProperty("user.dir"));
        }
    }

    private static String getHome() {
        return getProperty(ROOT);
    }

    public Config loadConfig() {
        InputStream input = getConfigFromProperty();
        if (input == null) {
            input = getConfigFromHome();
        }
        if (input == null) {
            input = getDefaultConfig();
        }
        Throwable error = null;
        Properties properties = new Properties();
        if (input != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Loading config");
            }
            try {
                properties.load(input);
                input.close();
            } catch (Throwable throwable) {
                error = throwable;
            }
        }
        if (input == null || error != null) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to load bootstrap config", error);
            }
        }
        return new PropertiesConfig(properties, replacer);
    }

    private static InputStream getConfigFromProperty() {
        InputStream stream;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Looking for custom config path under %s property", CONFIG));
            }
            String config = getProperty(CONFIG);
            if (config != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug(format("Opening custom config at %s", config));
                }
                stream = (new URL(config)).openStream();
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug(format("Custom config path under %s property is not set", CONFIG));
                }
                stream = null;
            }
        } catch (Throwable throwable) {
            stream = null;
        }
        return stream;
    }

    private static InputStream getConfigFromHome() {
        File path = new File(new File(getHome(), CONFIG_FOLDER), DEFAULT_CONFIG);
        InputStream stream;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Looking for config at %s", path));
            }
            stream = new FileInputStream(path);
        } catch (Throwable t) {
            stream = null;
        }
        if (stream != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Config found at %s", path));
            }
        } else {
            if (logger.isInfoEnabled()) {
                logger.info(format("Config can't be found at %s", path));
            }
        }
        return stream;
    }

    private static InputStream getDefaultConfig() {
        InputStream stream;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Looking for default config at %s", DEFAULT_CONFIG));
            }
            stream = PropertiesConfigLoader.class.getResourceAsStream(DEFAULT_CONFIG);
        } catch (Throwable throwable) {
            stream = null;
        }
        if (stream != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Default config found at %s", DEFAULT_CONFIG));
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Default config can't be found at %s", DEFAULT_CONFIG));
            }
        }
        return stream;
    }

    public Replacer getReplacer() {
        return replacer;
    }

    public void setReplacer(Replacer replacer) {
        this.replacer = replacer;
    }
}