package com.stone.iqiyilogweb;

import com.stone.iqiyilogweb.dao.CategoryClickCountDao;
import com.stone.iqiyilogweb.domain.CategoryClickCount;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import sun.applet.Main;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author stone
 * @date 2019/5/28 13:58
 * description
 */
@RestController
public class CategoryClickCountApp {

    private static Map<String,String> courses = new HashMap<>();
    static {
        courses.put("1","偶像爱情");
        courses.put("2","宫斗谋权");
        courses.put("3","玄幻史诗");
        courses.put("4", "都市生活");
        courses.put("5", "罪案谍战");
        courses.put("6", "历险科幻");
    }

    @RequestMapping(value = "clickCount",method = RequestMethod.POST)
    public List<CategoryClickCount> clickCountQuery() throws IOException {
        List<CategoryClickCount> list = CategoryClickCountDao.query("20190528");
        for(CategoryClickCount model:list){
            String name = courses.get(model.getName().substring(9));
            if(name != null){
                model.setName(name);
            } else {
                model.setName("其他");
            }
        }
        return list;
    }

    @RequestMapping(value = "clickCountChart",method = RequestMethod.GET)
    public ModelAndView clickCountChart() {
        return new ModelAndView("clickcountchart");
    }

//    public static void main(String[] args) throws IOException {
//        CategoryClickCountApp categoryClickCountApp = new CategoryClickCountApp();
//        List<CategoryClickCount> list = categoryClickCountApp.clickCountQuery();
//        for (CategoryClickCount c:list){
//            System.out.println(c.getName()+","+c.getValue());
//        }
//    }
}
