package org.apache.jena.riot.system;

import java.util.LinkedHashMap;

import org.junit.Assert;
import org.junit.Test;


public class TestPrefixMapStd {
    @Test
    public void testFirstPrefixWins() {
        PrefixMap pm = PrefixMapStd.builder().setLastPrefixWins(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("a:x", actual);
    }

    @Test
    public void testLastLastWins() {
        PrefixMap pm = PrefixMapStd.builder().setLastPrefixWins(true).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("b:x", actual);
    }

    @Test
    public void testFirstPrefixWinsLazy() {
        PrefixMap pm = PrefixMapStd.builder().setMapFactory(LinkedHashMap::new).setEagerIriToPrefix(false).setLastPrefixWins(false).setUseLocking(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("a:x", actual);
    }

    @Test
    public void testLastLastWinsLazy() {
        PrefixMap pm = PrefixMapStd.builder().setMapFactory(LinkedHashMap::new).setEagerIriToPrefix(false).setLastPrefixWins(true).setUseLocking(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/");
        String actual = pm.abbreviate("http://example.org/x");
        Assert.assertEquals("b:x", actual);
    }

    @Test
    public void testFastTrackEnabled() {
        PrefixMap pm = PrefixMapStd.builder().setEnableFastTrack(true).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/foo");
        String actual = pm.abbreviate("http://example.org/foobar");
        Assert.assertEquals("a:foobar", actual);
    }

    @Test
    public void testFastTrackDisabled() {
        PrefixMap pm = PrefixMapStd.builder().setEnableFastTrack(false).build();
        pm.add("a", "http://example.org/");
        pm.add("b", "http://example.org/foo");
        String actual = pm.abbreviate("http://example.org/foobar");
        Assert.assertEquals("b:bar", actual);
    }
}
