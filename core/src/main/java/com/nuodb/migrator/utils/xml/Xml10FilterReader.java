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
package com.nuodb.migrator.utils.xml;

import org.apache.xerces.util.XMLChar;
import org.slf4j.Logger;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * The filter reader which skips invalid xml characters.
 *
 * @author Sergey Bushik
 */
public class Xml10FilterReader extends FilterReader {

    protected final transient Logger logger = getLogger(getClass());

    public Xml10FilterReader(Reader in) {
        super(in);
    }

    @Override
    public int read(char[] buf, int off, int len) throws IOException {
        int read = super.read(buf, off, len);
        if (read == -1) {
            return -1;
        }
        int pos = off - 1;
        for (int readPos = off; readPos < off + read; readPos++) {
            char ch = buf[readPos];
            if (isValid(ch)) {
                pos++;
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(format("Filtered out &#%04x character", (int) ch));
                }
                continue;
            }
            if (pos < readPos) {
                buf[pos] = buf[readPos];
            }
        }
        return pos - off + 1;
    }

    protected boolean isValid(char c) {
        return XMLChar.isValid(c);
    }
}
