/**
 * Copyright (c) 2012, NuoDB, Inc.
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

import com.nuodb.migrator.bootstrap.config.Config;
import com.nuodb.migrator.bootstrap.config.PropertiesConfigLoader;
import com.nuodb.migrator.bootstrap.config.PropertiesReplacement;
import com.nuodb.migrator.bootstrap.config.Replacer;
import com.nuodb.migrator.bootstrap.log.Log;
import com.nuodb.migrator.bootstrap.log.LogFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;

import static com.nuodb.migrator.bootstrap.classpath.ClassPathLoaderUtils.createClassPathLoader;
import static com.nuodb.migrator.bootstrap.config.Config.*;
import static java.lang.System.setProperty;
import static java.lang.Thread.currentThread;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class Bootstrap {

    private static final Log log = LogFactory.getLog(Bootstrap.class);

    private ClassLoader classLoader;

    private static final int BOOT_ERROR = 1;

    private Config config;

    public void boot(String[] arguments) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Loading config");
        }
        config = loadConfig();

        if (log.isDebugEnabled()) {
            log.debug("Exposing migrator config to a system properties");
        }
        for (String property : config.getPropertyNames()) {
            setProperty(property, config.getProperty(property));
        }

        if (log.isDebugEnabled()) {
            log.debug("Creating class classpath");
        }
        classLoader = createClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);

        if (log.isDebugEnabled()) {
            log.debug("Creating bootable");
        }
        Bootable bootable = createBootable();
        bootable.boot(config, arguments);
    }

    protected Config loadConfig() {
        PropertiesConfigLoader configLoader = new PropertiesConfigLoader();
        configLoader.setReplacer(new Replacer(new PropertiesReplacement()));
        return configLoader.loadConfig();
    }

    protected ClassLoader createClassLoader() throws IOException {
        String classPath = getConfig().getProperty(CLASSPATH);
        if (classPath == null || classPath.equals("")) {
            return getClass().getClassLoader();
        }
        return createClassPathLoader(classPath, currentThread().getContextClassLoader());
    }

    protected Bootable createBootable() throws Exception {
        String bootableName = getConfig().getProperty(BOOTABLE_CLASS, DEFAULT_BOOTABLE_CLASS);
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
            if (log.isErrorEnabled()) {
                log.error("Execution failed", error);
            }
            System.exit(BOOT_ERROR);
        }
    }
}