/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.config;

import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static com.nuodb.migrator.config.Config.*;
import static java.lang.String.format;
import static java.lang.System.getProperties;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("unchecked")
public class PropertiesConfigLoader {

    private static final Logger logger = getLogger(PropertiesConfigLoader.class);

    static {
        setHome();
    }

    private static void setHome() {
        if (getProperty(HOME) != null) {
            return;
        }
        try {
            setProperty(HOME, new File(getProperty("user.dir"), "..").getCanonicalPath());
        } catch (IOException e) {
            setProperty(HOME, getProperty("user.dir"));
        }
    }

    private static String getHome() {
        return getProperty(HOME);
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
        Replacer replacer = new Replacer();
        replacer.addReplacements(getProperties());
        replacer.addReplacements(properties);
        return new PropertiesConfig(properties, replacer);
    }

    private static InputStream getConfigFromProperty() {
        InputStream stream;
        try {
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
        File path = new File(new File(getHome(), CONFIG_DIR), DEFAULT_CONFIG);
        InputStream stream;
        try {
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
}