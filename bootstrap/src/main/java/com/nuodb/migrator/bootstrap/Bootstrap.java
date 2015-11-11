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
package com.nuodb.migrator.bootstrap;

import com.nuodb.migrator.config.Config;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static com.nuodb.migrator.bootstrap.classpath.ClassPathLoaderUtils.createClassPathLoader;
import static com.nuodb.migrator.config.Config.*;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class Bootstrap {

    static {
        String logDatePattern = getProperty(LOG_DATE_PATTERN);
        if (logDatePattern != null && !logDatePattern.isEmpty()) {
            String logDate = new SimpleDateFormat(logDatePattern).format(new Date());
            setProperty(LOG_DATE, logDate);
        }
    }

    private static final Logger logger = getLogger(Bootstrap.class);

    private ClassLoader classLoader;

    private static final int BOOT_ERROR = 1;

    private Config config;

    public void boot(String[] arguments) throws Exception {
        config = Config.getInstance();

        Properties properties = config.getProperties();
        if (logger.isDebugEnabled()) {
            logger.debug(format("Exposing bootstrap config as system properties %s", properties));
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            setProperty((String) entry.getKey(), (String) entry.getValue());
        }
        classLoader = createClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);

        Bootable bootable = createBootable();
        bootable.boot(arguments);
    }

    protected ClassLoader createClassLoader() throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating class loader");
        }
        String classPath = getConfig().getProperty(CLASSPATH);
        if (classPath == null || classPath.equals("")) {
            return getClass().getClassLoader();
        }
        return createClassPathLoader(classPath, currentThread().getContextClassLoader());
    }

    protected Bootable createBootable() throws Exception {
        String bootableName = getConfig().getProperty(BOOTABLE_CLASS, DEFAULT_BOOTABLE_CLASS);
        if (logger.isDebugEnabled()) {
            logger.debug(format("Booting %s", bootableName));
        }
        Class<? extends Bootable> bootableType = (Class<? extends Bootable>) getClassLoader().loadClass(bootableName);
        Constructor<? extends Bootable> constructor = bootableType.getConstructor();
        return constructor.newInstance();
    }

    private Config getConfig() {
        return config;
    }

    private ClassLoader getClassLoader() {
        return classLoader;
    }

    public static void main(String[] arguments) {
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.boot(arguments);
        } catch (Throwable error) {
            if (logger.isErrorEnabled()) {
                logger.error("Execution failed", error);
            }
            System.exit(BOOT_ERROR);
        }
    }
}