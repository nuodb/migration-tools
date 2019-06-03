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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.primitives.Bytes.asList;
import static com.google.common.primitives.Bytes.toArray;
import static org.apache.commons.codec.binary.Hex.encodeHex;

/**
 * @author Sergey Bushik
 */
class MockBlob implements Blob {

    private List<Byte> buffer = newArrayList();
    private boolean released;

    public MockBlob() {
    }

    public MockBlob(byte[] data) {
        buffer = asList(data);
    }

    public long length() throws SQLException {
        validate();
        return buffer.size();
    }

    public byte[] getBytes(long position, int length) throws SQLException {
        return getBytes(position, (long) length);
    }

    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(getBytes(1, length()));
    }

    public InputStream getBinaryStream(long position, long length) throws SQLException {
        return new ByteArrayInputStream(getBytes(position, length));
    }

    public long position(byte[] pattern, long start) throws SQLException {
        validate();
        int index = indexOf(toArray(buffer), pattern, (int) (start - 1));
        if (index != -1) {
            index += 1;
        }
        return index;
    }

    public long position(Blob pattern, long start) throws SQLException {
        return position(pattern.getBytes(1, (int) pattern.length()), start);
    }

    public int setBytes(long position, byte[] bytes) throws SQLException {
        return setBytes(position, bytes, 0, bytes.length);
    }

    public int setBytes(long position, byte[] bytes, int offset, int length) throws SQLException {
        validate();
        buffer.addAll((int) (position - 1), asList(bytes).subList(offset, offset + length));
        return length;
    }

    public OutputStream setBinaryStream(long position) throws SQLException {
        validate();
        return new BlobOutputStream((int) (position - 1));
    }

    public void truncate(long length) throws SQLException {
        validate();
        buffer = buffer.subList(0, (int) length);
    }

    public void free() throws SQLException {
        released = true;
    }

    public int hashCode() {
        return buffer.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Blob))
            return false;
        Blob blob = (Blob) o;
        try {
            return StreamUtils.equals(getBinaryStream(), blob.getBinaryStream());
        } catch (IOException exception) {
            throw new JdbcTypeException(exception);
        } catch (SQLException exception) {
            throw new JdbcTypeException(exception);
        }
    }

    public String toString() {
        return new String(encodeHex(toArray(buffer), false));
    }

    private static int indexOf(byte[] buffer, byte[] pattern, int start) {
        if (pattern.length == 0) {
            return 0;
        }
        outer: for (int i = start; i < buffer.length - pattern.length + 1; i++) {
            for (int j = 0; j < pattern.length; j++) {
                if (buffer[i + j] != pattern[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private void validate() throws SQLException {
        if (released) {
            throw new SQLException("Memory buffer was released");
        }
    }

    private byte[] getBytes(long position, long length) throws SQLException {
        validate();
        length = validateLength(position, (int) length);
        long fromIndex = position - 1;
        long toIndex = fromIndex + length;
        return toArray(buffer.subList((int) fromIndex, (int) toIndex));
    }

    private int validateLength(long position, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("Length must be greater or equals to 0");
        }
        if ((length + (position - 1)) > buffer.size()) {
            return buffer.size() - (int) (position - 1);
        }
        return length;
    }

    private class BlobOutputStream extends OutputStream {
        private int index;

        public BlobOutputStream(int index) {
            this.index = index;
        }

        public void write(int value) throws IOException {
            buffer.add(index, (byte) value);
            index++;
        }
    }
}