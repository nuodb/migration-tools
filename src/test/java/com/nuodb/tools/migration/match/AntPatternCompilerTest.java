package com.nuodb.tools.migration.match;

import junit.framework.Assert;
import org.junit.Test;

public class AntPatternCompilerTest {
    final String patternTest = "TEST*";


    @Test
    public void testMatcherCreation() throws Exception {

        final Matcher matcher = (new AntPatternCompiler()).matcher(patternTest);
        Assert.assertNotNull(matcher);
        Assert.assertNotNull((new AntPatternCompiler()).matcher("*TEST*"));
        Assert.assertNotNull((new AntPatternCompiler()).matcher("TEST*"));
        Assert.assertEquals(matcher.pattern(), patternTest);

    }

    @Test
    public void testMatch() throws Exception {
        AntPatternCompiler compiler = new AntPatternCompiler();
        Matcher matcher = compiler.matcher(patternTest);
        Assert.assertTrue(matcher.matches("TEST3"));
        Assert.assertTrue(matcher.matches("TEST"));
        Assert.assertTrue(matcher.matches("TEST tt"));
        Assert.assertFalse(matcher.matches("TES tt"));
        Assert.assertFalse(matcher.matches(""));
        Assert.assertFalse(matcher.matches("test1"));
    }
}
