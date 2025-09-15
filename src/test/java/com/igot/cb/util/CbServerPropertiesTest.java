package com.igot.cb.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CbServerPropertiesTest {

    @Test
    void testRecentSearchesLimitField() throws Exception {
        CbServerProperties props = new CbServerProperties();

        // Use reflection to set the private field
        Field field = CbServerProperties.class.getDeclaredField("recentSearchesLimit");
        field.setAccessible(true);
        field.set(props, 15);

        // Read back the field value
        Integer value = (Integer) field.get(props);

        assertEquals(15, value);
    }
}

