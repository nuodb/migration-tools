package com.nuodb.migrator.bootstrap.loader;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;

public class DynamicClassLoader extends URLClassLoader {

    public DynamicClassLoader(Collection<URL> urls) {
        super(urls.toArray(new URL[urls.size()]));
    }

    public DynamicClassLoader(Collection<URL> urls, ClassLoader parent) {
        super(urls.toArray(new URL[urls.size()]), parent);
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
    }

    @Override
    public URL[] getURLs() {
        return super.getURLs();
    }
}
