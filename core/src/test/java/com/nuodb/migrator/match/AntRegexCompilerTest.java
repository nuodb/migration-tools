package com.nuodb.migrator.match;

import org.junit.Test;

import static junit.framework.Assert.*;

public class AntRegexCompilerTest {

    private static final String PATTERN = "TEST*";

    @Test
    public void testMatcherCreation() throws Exception {
        RegexCompiler compiler = AntRegexCompiler.INSTANCE;
        Regex matcher = compiler.compile(PATTERN);
        assertNotNull(matcher);
        assertNotNull(compiler.compile("*TEST*"));
        assertEquals(matcher.regex(), PATTERN);
    }

    @Test
    public void testMatch() throws Exception {
        RegexCompiler compiler = AntRegexCompiler.INSTANCE;
        Regex regex = compiler.compile(PATTERN);
        assertTrue(regex.test("TEST3"));
        assertTrue(regex.test("TEST"));
        assertTrue(regex.test("TEST tt"));
        assertFalse(regex.test("TES tt"));
        assertFalse(regex.test(""));
        assertFalse(regex.test("test1"));
    }
}
