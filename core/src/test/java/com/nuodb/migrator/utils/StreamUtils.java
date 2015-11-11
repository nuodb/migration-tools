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
package com.nuodb.migrator.utils;

import com.nuodb.migrator.MigratorException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.CharBuffer;

import static org.apache.commons.io.IOUtils.contentEquals;

/**
 * @author Sergey Bushik
 */
public class StreamUtils {

    public static boolean equals(Reader reader1, Reader reader2) throws IOException {
        return contentEquals(reader1, reader2);
    }

    public static boolean equals(InputStream input1, InputStream input2) throws IOException {
        return contentEquals(input1, input2);
    }

    public static Reader newEqualsReader(Reader reader) {
        return new EqualsReader(reader);
    }

    public static InputStream newEqualsInputStream(InputStream inputStream) {
        return new EqualsInputStream(inputStream);
    }

    private static class EqualsReader extends Reader {

        private final Reader reader;

        public EqualsReader(Reader reader) {
            this.reader = reader;
        }

        @Override
        public int read(CharBuffer target) throws IOException {
            return reader.read(target);
        }

        @Override
        public int read() throws IOException {
            return reader.read();
        }

        @Override
        public int read(char[] buffer) throws IOException {
            return reader.read(buffer);
        }

        @Override
        public int read(char[] buffer, int off, int len) throws IOException {
            return reader.read(buffer, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return reader.skip(n);
        }

        @Override
        public boolean ready() throws IOException {
            return reader.ready();
        }

        @Override
        public boolean markSupported() {
            return reader.markSupported();
        }

        @Override
        public void mark(int readLimit) throws IOException {
            reader.mark(readLimit);
        }

        @Override
        public void reset() throws IOException {
            reader.reset();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public int hashCode() {
            return reader.hashCode();
        }

        @Override
        public String toString() {
            return reader.toString();
        }

        @Override
        public boolean equals(Object other) {
            try {
                return other instanceof Reader && StreamUtils.equals(this, (Reader) other);
            } catch (IOException exception) {
                throw new MigratorException(exception);
            }
        }
    }

    private static class EqualsInputStream extends InputStream {

        private final InputStream input;

        private EqualsInputStream(InputStream input) {
            this.input = input;
        }

        @Override
        public int read() throws IOException {
            return input.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return input.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return input.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return input.skip(n);
        }

        @Override
        public int available() throws IOException {
            return input.available();
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        @Override
        public void mark(int readLimit) {
            input.mark(readLimit);
        }

        @Override
        public void reset() throws IOException {
            input.reset();
        }

        @Override
        public boolean markSupported() {
            return input.markSupported();
        }

        @Override
        public boolean equals(Object other) {
            try {
                return other instanceof InputStream && StreamUtils.equals(this, (InputStream) other);
            } catch (IOException exception) {
                throw new MigratorException(exception);
            }
        }

        @Override
        public int hashCode() {
            return input.hashCode();
        }
    }
}
