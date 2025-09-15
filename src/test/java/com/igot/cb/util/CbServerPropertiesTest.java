package com.igot.cb.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CbServerPropertiesTest {

    @Test
    void testRecentSearchesLimitField() throws Exception {
        CbServerProperties props = new CbServerProperties();
        Field field = CbServerProperties.class.getDeclaredField("recentSearchesLimit");
        field.setAccessible(true);
        field.set(props, 15);
        Integer value = (Integer) field.get(props);
        assertEquals(15, value);
    }

    @Test
    void testGetterSetter() {
        CbServerProperties props = new CbServerProperties();
        props.setRecentSearchesLimit(25);
        Integer value = props.getRecentSearchesLimit();
        assertEquals(25, value);
    }
}

