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
package com.nuodb.migrator.jdbc.dialect;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;
import static com.google.common.io.Closeables.closeQuietly;
import static java.lang.String.format;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.unmodifiableSet;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SQLKeywords implements Set<String> {

    public static final SQLKeywords NONE_KEYWORDS = new SQLKeywords(EMPTY_SET, false);

    public static final SQLKeywords SQL_92_KEYWORDS = new SQLKeywords("sql92.keywords");

    public static final SQLKeywords SQL_99_KEYWORDS = new SQLKeywords("sql99.keywords");

    public static final SQLKeywords SQL_2003_KEYWORDS = new SQLKeywords("sql2003.keywords");

    public static final SQLKeywords NUODB_KEYWORDS = new SQLKeywords("nuodb.keywords");

    private Set<String> keywords = newTreeSet(String.CASE_INSENSITIVE_ORDER);

    private static Collection<String> load(String resource) {
        Collection<String> keywords = newHashSet();
        InputStream input = SQLKeywords.class.getResourceAsStream(resource);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String keyword;
        try {
            while ((keyword = reader.readLine()) != null) {
                keyword = keyword.trim();
                if (keyword.length() != 0) {
                    keywords.add(keyword);
                }
            }
        } catch (IOException exception) {
            throw new DialectException(format("Failed loading reserved key words from %s", resource), exception);
        } finally {
            closeQuietly(reader);
        }
        return keywords;
    }

    private SQLKeywords(String resource) {
        this(load(resource), false);
    }

    public SQLKeywords() {
    }

    public SQLKeywords(Collection<String> keywords) {
        this(keywords, true);
    }

    public SQLKeywords(Collection<String> keywords, boolean modifiable) {
        this.keywords.addAll(keywords);
        if (!modifiable) {
            this.keywords = unmodifiableSet(this.keywords);
        }
    }

    @Override
    public int size() {
        return keywords.size();
    }

    @Override
    public boolean isEmpty() {
        return keywords.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return keywords.contains(o);
    }

    @Override
    public Iterator<String> iterator() {
        return keywords.iterator();
    }

    @Override
    public Object[] toArray() {
        return keywords.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return keywords.toArray(a);
    }

    @Override
    public boolean add(String s) {
        return keywords.add(s);
    }

    @Override
    public boolean remove(Object o) {
        return keywords.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return keywords.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        return keywords.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return keywords.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return keywords.removeAll(c);
    }

    @Override
    public void clear() {
        keywords.clear();
    }

    @Override
    public boolean equals(Object o) {
        return keywords.equals(o);
    }

    @Override
    public int hashCode() {
        return keywords.hashCode();
    }

    @Override
    public String toString() {
        return keywords.toString();
    }
}
