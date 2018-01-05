package com.vehicle.common.utils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author jerry
 */
public class ComparatorUtils {
    /**
     * 根据Value排序Entry
     *
     * @param list
     * @return
     */
    public static List<Map.Entry<String, Integer>> sort(List<Map.Entry<String, Integer>> list) {
        List<Map.Entry<String, Integer>> result = new ArrayList<>(list.size());
        result.addAll(list);
        result.sort(Map.Entry.comparingByValue());
        return Lists.reverse(result);
    }
}
