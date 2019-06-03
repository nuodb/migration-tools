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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.utils.StreamUtils;
import org.apache.commons.io.Charsets;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
class MockClob implements Clob {

    private static final transient Charset BINARY_CHARSET = Charsets.toCharset("ISO-8859-1");
    private StringBuffer buffer = new StringBuffer();
    private boolean released;

    public MockClob() {
    }

    public MockClob(String buffer) {
        this.buffer.append(buffer);
    }

    public long length() throws SQLException {
        return buffer.length();
    }

    public void truncate(long length) throws SQLException {
        validate();
        buffer.setLength((int) length);
    }

    public InputStream getAsciiStream() throws SQLException {
        validate();
        return new ByteArrayInputStream(buffer.toString().getBytes(BINARY_CHARSET));
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        validate();
        return new ClobOutputStream((int) (pos - 1));
    }

    public Reader getCharacterStream() throws SQLException {
        validate();
        return new StringReader(buffer.toString());
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        validate();
        length = validateLength(pos, (int) length);
        return new StringReader(getSubString(pos, (int) length));
    }

    public Writer setCharacterStream(long position) throws SQLException {
        validate();
        return new MemoryClobWriter((int) (position - 1));
    }

    public String getSubString(long position, int length) throws SQLException {
        validate();
        length = validateLength(position, length);
        return buffer.substring((int) (position - 1), (int) (position - 1) + length);
    }

    public int setString(long position, String str) throws SQLException {
        return setString(position, str, 0, str.length());
    }

    public int setString(long position, String str, int offset, int len) throws SQLException {
        validate();
        str = str.substring(offset, offset + len);
        buffer.replace((int) (position - 1), (int) (position - 1) + str.length(), str);
        return len;
    }

    public long position(String pattern, long start) throws SQLException {
        validate();
        int index = buffer.toString().indexOf(pattern, (int) (start - 1));
        if (-1 != index)
            index += 1;
        return index;
    }

    public long position(Clob pattern, long start) throws SQLException {
        return position(pattern.getSubString(1, (int) pattern.length()), start);
    }

    private void validate() throws SQLException {
        if (released) {
            throw new SQLException("Memory buffer was released");
        }
    }

    public void free() throws SQLException {
        released = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Clob))
            return false;
        Clob clob = (Clob) o;
        try {
            return StreamUtils.equals(getAsciiStream(), clob.getAsciiStream());
        } catch (IOException exception) {
            throw new JdbcTypeException(exception);
        } catch (SQLException exception) {
            throw new JdbcTypeException(exception);
        }
    }

    public int hashCode() {
        return buffer.toString().hashCode();
    }

    public String toString() {
        return buffer.toString();
    }

    private int validateLength(long position, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length must be greater or equals to 0");
        }
        if ((length + (position - 1)) > buffer.length()) {
            return buffer.length() - (int) (position - 1);
        }
        return length;
    }

    private class MemoryClobWriter extends Writer {

        private int index;

        public MemoryClobWriter(int index) {
            this.index = index;
        }

        public void close() throws IOException {
        }

        public void flush() throws IOException {
        }

        public void write(char[] buffer, int off, int len) throws IOException {
            try {
                setString(index + 1, new String(buffer, off, len));
            } catch (SQLException exception) {
                throw new IOException(exception.getMessage());
            }
            index++;
        }
    }

    private class ClobOutputStream extends OutputStream {

        private int index;

        public ClobOutputStream(int index) {
            this.index = index;
        }

        public void write(int byteValue) throws IOException {
            byte[] bytes = new byte[] { (byte) byteValue };
            try {
                setString(index + 1, new String(bytes));
            } catch (SQLException exception) {
                throw new IOException(exception.getMessage());
            }
            index++;
        }
    }
}
