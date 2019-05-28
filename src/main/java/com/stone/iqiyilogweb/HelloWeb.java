package com.stone.iqiyilogweb;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

/**
 * @author stone
 * @date 2019/5/28 10:04
 * description
 */
@RestController
public class HelloWeb {

    @RequestMapping(value = "hello",method = RequestMethod.GET)
    public String hello(){
        return "hello springboot";
    }


    @RequestMapping(value = "firstDemo",method = RequestMethod.GET)
        public ModelAndView firstDemo(){
            return new ModelAndView("test");
    }
}
