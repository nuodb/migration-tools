package com.nuodb.migrator.bootstrap.classpath;

import com.nuodb.migrator.bootstrap.log.Log;
import com.nuodb.migrator.bootstrap.log.LogFactory;

import java.net.URL;
import java.net.URLClassLoader;

import static com.nuodb.migrator.bootstrap.classpath.DirClassPath.toDirClassPath;
import static com.nuodb.migrator.bootstrap.classpath.JarDirClassPath.toJarDirClassPath;
import static com.nuodb.migrator.bootstrap.classpath.UrlClassPath.toUrlClassPath;
import static java.lang.String.format;

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
        addClassPath(toDirClassPath(path));
    }

    public void addJar(String path) {
        addClassPath(toJarDirClassPath(path));
    }

    public void addJarDir(String path) {
        addClassPath(toJarDirClassPath(path));
    }

    public void addClassPath(ClassPath classPath) {
        if (log.isTraceEnabled()) {
            log.trace(format("Adding class path %1$s", classPath));
        }
        classPath.exposeClassPath(this);
    }
}
