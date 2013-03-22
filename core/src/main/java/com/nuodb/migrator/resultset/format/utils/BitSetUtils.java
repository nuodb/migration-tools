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
package com.nuodb.migrator.resultset.format.utils;

import java.util.BitSet;

import static java.lang.Character.digit;
import static java.lang.Character.forDigit;

/**
 * @author Sergey Bushik
 */
public class BitSetUtils {

    public static String toHex(BitSet bitSet) {
        int length = bitSet.length();
        int digits = (length / 4) + (length % 4 > 0 ? 1 : 0);
        int index = digits - 1;
        char[] value = new char[digits];
        for (int bit = 0; bit < length; index--) {
            int hex = 0;
            hex |= bitSet.get(bit++) ? 0x1 : 0;
            hex |= bitSet.get(bit++) ? 0x2 : 0;
            hex |= bitSet.get(bit++) ? 0x4 : 0;
            hex |= bitSet.get(bit++) ? 0x8 : 0;
            value[index] = forDigit(hex, 0x10);
        }
        return new String(value);
    }

    public static BitSet fromHex(String value) {
        char[] hex = value.toCharArray();
        BitSet bitSet = new BitSet(hex.length << 2);
        int bit = 0;
        for (int index = hex.length - 1; index >= 0; index--) {
            int digit = digit(hex[index], 0x10);
            bitSet.set(bit++, (digit & 0x1) > 0);
            bitSet.set(bit++, (digit & 0x2) > 0);
            bitSet.set(bit++, (digit & 0x4) > 0);
            bitSet.set(bit++, (digit & 0x8) > 0);
        }
        return bitSet;
    }
}
