package com.vehicle.common.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author jerry
 */
public class ConfigUtils {
    private static final Properties PROPERTIES = new Properties();

    static {
        try {
            String resource = ConfigUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            assert resource != null;

            File resources = new File(resource);
            if (resources.isDirectory()) {
                File[] files = resources.listFiles();

                assert files != null;
                for (File file : files) {
                    if (file.getName().endsWith(".properties")) {
                        Properties properties = new Properties();
                        properties.load(new FileInputStream(file));
                        PROPERTIES.putAll(properties);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getString(String key, String defaultValue) {
        String value = PROPERTIES.getProperty(key);
        return StringUtils.isEmpty(value) ? defaultValue : value;
    }

    public static int getInt(String key, int defaultValue) {
        String value = PROPERTIES.getProperty(key);
        if (NumberUtils.isDigits(value)) {
            return NumberUtils.toInt(value);
        }
        return defaultValue;
    }

    public static double getDouble(String key, double defaultValue) {
        String value = PROPERTIES.getProperty(key);
        if (NumberUtils.isDigits(value)) {
            return NumberUtils.toDouble(value);
        }
        return defaultValue;
    }

    public static String getProperty(String key) {
        return PROPERTIES.getProperty(key);
    }
}
