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
package com.nuodb.migrator.bootstrap.classpath;

import org.slf4j.Logger;

import java.net.URL;
import java.net.URLClassLoader;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class ClassPathLoader extends URLClassLoader {

    private static final Logger logger = getLogger(ClassPathLoader.class);

    public ClassPathLoader() {
        super(new URL[] {});
    }

    public ClassPathLoader(ClassLoader parent) {
        super(new URL[] {}, parent);
    }

    public boolean addUrl(String path) {
        try {
            addClassPath(new UrlClassPath(path));
            return true;
        } catch (ClassPathException exception) {
            return false;
        }
    }

    public boolean addDir(String path) {
        try {
            addClassPath(new DirClassPath(path));
            return true;
        } catch (ClassPathException exception) {
            return false;
        }
    }

    public boolean addJar(String path) {
        try {
            addClassPath(new JarClassPath(path));
            return true;
        } catch (ClassPathException exception) {
            return false;
        }
    }

    public boolean addJarDir(String path) {
        try {
            addClassPath(new JarDirClassPath(path));
            return true;
        } catch (ClassPathException exception) {
            return false;
        }
    }

    public void addClassPath(ClassPath classPath) {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Adding class path %s", classPath));
        }
        classPath.exposeClassPath(this);
    }

    public void addUrl(URL url) {
        addURL(url);
    }
}
