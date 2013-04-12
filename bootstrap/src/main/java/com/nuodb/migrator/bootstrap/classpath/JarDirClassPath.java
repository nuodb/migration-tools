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

import com.nuodb.migrator.bootstrap.classpath.filter.AndFileFilter;
import com.nuodb.migrator.bootstrap.classpath.filter.JarFileFilter;
import com.nuodb.migrator.bootstrap.classpath.filter.WildcardFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import static com.nuodb.migrator.bootstrap.classpath.filter.WildcardFileFilter.WILDCARDS;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class JarDirClassPath implements FileClassPath {

    private File dir;
    private FileFilter fileFilter;

    public JarDirClassPath(File dir) {
        this(dir, JarFileFilter.INSTANCE);
    }

    public JarDirClassPath(File dir, FileFilter fileFilter) {
        this.dir = dir;
        this.fileFilter = fileFilter;
    }

    @Override
    public void exposeClassPath(ClassPathLoader classPathLoader) {
        for (File file : dir.listFiles(getFileFilter())) {
            try {
                classPathLoader.addUrl(file.toURI().toURL());
            } catch (IOException exception) {
                throw new ClassPathException(exception);
            }
        }
    }

    public static JarDirClassPath toJarDirClassPath(String path) {
        int fileSeparator = Math.max(path.lastIndexOf(WINDOWS_FILE_SEPARATOR), path.lastIndexOf(UNIX_FILE_SEPARATOR));
        String base = path;
        String name = null;
        if (fileSeparator != -1) {
            base = path.substring(0, fileSeparator);
            name = path.substring(fileSeparator + 1);
        }
        File dir = new File(base);
        if (!dir.exists() || !dir.isDirectory() || !dir.canRead()) {
            throw new ClassPathException(format("%1$s is not a valid directory", path));
        }
        if (name == null || !WILDCARDS.matcher(name).find()) {
            throw new ClassPathException(format("Directory %1$s has not a valid file name nor a pattern", path));
        }
        return new JarDirClassPath(dir, new AndFileFilter(JarFileFilter.INSTANCE, new WildcardFileFilter(name)));
    }

    public FileFilter getFileFilter() {
        return fileFilter;
    }

    public void setFileFilter(FileFilter fileFilter) {
        this.fileFilter = fileFilter;
    }
}
