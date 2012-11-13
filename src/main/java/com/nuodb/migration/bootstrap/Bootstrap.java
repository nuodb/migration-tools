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

import com.google.common.collect.Maps;
import com.nuodb.migration.bootstrap.loader.DynamicClassLoaderFactory;
import com.nuodb.migration.bootstrap.loader.DynamicClassLoaderType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * @author Sergey Bushik
 */
public class Bootstrap extends DynamicClassLoaderFactory {

    private static final Log log = LogFactory.getLog(Bootstrap.class);

    private static final String BOOTSTRAP_LOADER = "com.nuodb.migration.bootstrap.loader";

    private ClassLoader classLoader;

    public void boot() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Initializing bootstrap class loader");
        }
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        classLoader = createClassLoader(BootstrapConfig.getProperty(BOOTSTRAP_LOADER), parent);
        Thread.currentThread().setContextClassLoader(classLoader);

        if (log.isDebugEnabled()) {
            log.debug("Expanding system properties");
        }
        Properties properties = System.getProperties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                entry.setValue(BootstrapConfig.expand((String) value));
            }
        }
    }

    public void expand(String[] arguments) {
        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = BootstrapConfig.expand(arguments[i]);
        }
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    protected ClassLoader createClassLoader(String loader, ClassLoader parent) throws IOException {
        if (loader == null || loader.equals("")) {
            return getClass().getClassLoader();
        }
        Map<String, DynamicClassLoaderType> loaderTypes = Maps.newLinkedHashMap();
        for (StringTokenizer tokenizer = new StringTokenizer(loader, ","); tokenizer.hasMoreElements(); ) {
            String repository = BootstrapConfig.expand(tokenizer.nextToken());
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
        return createClassLoader(loaderTypes, parent);
    }
}