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
package com.nuodb.migration.bootstrap;

import com.nuodb.migration.bootstrap.config.Config;
import com.nuodb.migration.bootstrap.config.PropertiesConfigLoader;
import com.nuodb.migration.bootstrap.config.Replacer;
import com.nuodb.migration.bootstrap.config.PropertiesReplacement;
import com.nuodb.migration.bootstrap.loader.DynamicClassLoaderFactory;
import com.nuodb.migration.bootstrap.loader.DynamicClassLoaderType;
import com.nuodb.migration.bootstrap.log.Log;
import com.nuodb.migration.bootstrap.log.LogFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import static com.nuodb.migration.bootstrap.config.Config.BOOTABLE;
import static com.nuodb.migration.bootstrap.config.Config.LOADER;

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
            log.debug("Creating class loader");
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
        Replacer replacer = new Replacer();
        replacer.setReplacement(new PropertiesReplacement(System.getProperties()));

        PropertiesConfigLoader loader = new PropertiesConfigLoader();
        loader.setReplacer(replacer);
        return loader.loadConfig();
    }

    protected ClassLoader createClassLoader() throws IOException {
        String loader = getConfig().getProperty(LOADER);
        if (loader == null || loader.equals("")) {
            return getClass().getClassLoader();
        }
        Map<String, DynamicClassLoaderType> loaderTypes = new LinkedHashMap<String, DynamicClassLoaderType>();
        for (StringTokenizer tokenizer = new StringTokenizer(loader, ","); tokenizer.hasMoreElements(); ) {
            String repository = tokenizer.nextToken();
            try {
                new URL(repository);
                loaderTypes.put(repository, DynamicClassLoaderType.URL);
            } catch (MalformedURLException exception) {
                if (repository.endsWith("*.jar")) {
                    repository = repository.substring(0, repository.length() - "*.jar".length());
                    loaderTypes.put(repository, DynamicClassLoaderType.JAR_DIR);
                } else if (repository.endsWith(".jar")) {
                    loaderTypes.put(repository, DynamicClassLoaderType.JAR);
                } else {
                    loaderTypes.put(repository, DynamicClassLoaderType.DIR);
                }
            }
        }
        return DynamicClassLoaderFactory.createClassLoader(loaderTypes, Thread.currentThread().getContextClassLoader());
    }

    protected Bootable createBootable() throws Exception {
        String bootableName = getConfig().getProperty(BOOTABLE);
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
                log.error("Boot failed", error);
            }
            System.exit(BOOT_ERROR);
        }
    }
}