package com.nuodb.migrator.bootstrap.config;

import com.nuodb.migrator.bootstrap.log.Log;
import com.nuodb.migrator.bootstrap.log.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static com.nuodb.migrator.bootstrap.config.Config.*;

@SuppressWarnings("unchecked")
public class PropertiesConfigLoader {

    private static final Log log = LogFactory.getLog(PropertiesConfigLoader.class);

    private Replacer replacer;

    static {
        setHome();
    }

    private static void setHome() {
        if (System.getProperty(ROOT) != null) {
            return;
        }
        try {
            System.setProperty(ROOT, new File(System.getProperty("user.dir"), "..").getCanonicalPath());
        } catch (IOException e) {
            System.setProperty(ROOT, System.getProperty("user.dir"));
        }
    }

    private static String getHome() {
        return System.getProperty(ROOT);
    }

    public Config loadConfig() {
        InputStream is = getConfigFromProperty();
        if (is == null) {
            is = getConfigFromHome();
        }
        if (is == null) {
            is = getDefaultConfig();
        }
        Throwable error = null;
        Properties properties = new Properties();
        if (is != null)
            try {
                properties.load(is);
                is.close();
            } catch (Throwable throwable) {
                error = throwable;
            }
        if (is == null || error != null) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to load bootstrap config", error);
            }
        }
        return new PropertiesConfig(properties, replacer);
    }

    private static InputStream getConfigFromProperty() {
        InputStream stream = null;
        try {
            String config = System.getProperty(CONFIG);
            if (config != null) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Attempting to load properties from %s", config));
                }
                stream = (new URL(config)).openStream();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("No config file specified as an argument of system property %s",
                            CONFIG));
                }
            }
        } catch (Throwable throwable) {
            // ignored
        }
        return stream;
    }

    private static InputStream getConfigFromHome() {
        InputStream stream = null;
        File properties = null;
        try {
            properties = new File(new File(getHome(), CONFIG_FOLDER), DEFAULT_CONFIG);
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

    private static InputStream getDefaultConfig() {
        InputStream stream = null;
        try {
            String config = DEFAULT_CONFIG;
            stream = PropertiesConfigLoader.class.getResourceAsStream(config);
            if (stream != null) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Default bootstrap config found at %s", config));
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Default bootstrap config can't be found at %s", config));
                }
            }
        } catch (Throwable throwable) {
            // ignored
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
