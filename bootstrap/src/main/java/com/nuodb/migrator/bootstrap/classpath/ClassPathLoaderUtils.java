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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class ClassPathLoaderUtils {

    private static final Logger logger = getLogger(ClassPathLoaderUtils.class);

    public static final String PATH_SEPARATOR = ",";

    public static ClassPathLoader createClassPathLoader(String path, ClassLoader parent) {
        Set<String> paths = new LinkedHashSet<String>();
        for (StringTokenizer tokenizer = new StringTokenizer(path, PATH_SEPARATOR); tokenizer.hasMoreElements();) {
            paths.add(tokenizer.nextToken());
        }
        return createClassPathLoader(paths, parent);
    }

    public static ClassPathLoader createClassPathLoader(Collection<String> paths, ClassLoader parent) {
        ClassPathLoader classPathLoader = parent != null ? new ClassPathLoader() : new ClassPathLoader(parent);
        for (String path : paths) {
            if ((classPathLoader.addUrl(path) || classPathLoader.addJar(path) || classPathLoader.addJarDir(path)
                    || classPathLoader.addDir(path))) {
                // alright, the path was recognized
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug(format("Path is neither one of the recognized class path types %s", path));
                }
            }
        }
        return classPathLoader;
    }
}
