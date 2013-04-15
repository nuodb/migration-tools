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
package com.nuodb.migrator.bootstrap.classpath;

import com.nuodb.migrator.bootstrap.log.Log;
import com.nuodb.migrator.bootstrap.log.LogFactory;

import java.net.URL;
import java.net.URLClassLoader;

import static com.nuodb.migrator.bootstrap.classpath.UrlClassPath.toUrlClassPath;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class ClassPathLoader extends URLClassLoader {

    private final Log log = LogFactory.getLog(ClassPathLoader.class);

    public ClassPathLoader() {
        super(new URL[]{});
    }

    public ClassPathLoader(ClassLoader parent) {
        super(new URL[]{}, parent);
    }

    public void addUrl(URL url) {
        addURL(url);
    }

    public void addUrl(String path) {
        addClassPath(toUrlClassPath(path));
    }

    public void addDir(String path) {
        addClassPath(new DirClassPath(path));
    }

    public void addJar(String path) {
        addClassPath(new JarDirClassPath(path));
    }

    public void addJarDir(String path) {
        addClassPath(new JarDirClassPath(path));
    }

    public void addClassPath(ClassPath classPath) {
        if (log.isTraceEnabled()) {
            log.trace(format("Adding class path %1$s", classPath));
        }
        classPath.exposeClassPath(this);
    }
}
