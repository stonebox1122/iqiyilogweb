package com.stone.iqiyilogweb.dao;

import com.stone.iqiyilogweb.domain.CategoryClickCount;
import com.stone.iqiyilogweb.utils.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author stone
 * @date 2019/5/28 11:45
 * description
 */
public class CategoryClickCountDao {

    public static List<CategoryClickCount> query(String dayStr) throws IOException {
        List<CategoryClickCount> list = new ArrayList<>();
        Map<String, Long> map = HBaseUtils.query("iqiyi.category_clickcount", dayStr, "info", "click_count");
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            CategoryClickCount categoryClickCount = new CategoryClickCount();
            categoryClickCount.setName(entry.getKey());
            categoryClickCount.setValue(entry.getValue());
            list.add(categoryClickCount);
        }
        return list;
    }

    public static void main(String[] args) throws IOException {
        List<CategoryClickCount> list = CategoryClickCountDao.query("20190528");
        System.out.println("=======================");
        for (CategoryClickCount c : list) {
            System.out.println(c.getName() + "," + c.getValue());
        }
        System.out.println("=======================");
    }

}
