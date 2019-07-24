package com.daniel51.dicts.data_service.controller;/*
 @author Daniel51
 @DESCRIPTION add a,b
 @create 2019/7/22
*/

import com.daniel51.dicts.data_service.pojo.Person;
import com.daniel51.dicts.data_service.service.AddServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AddController {
    @Autowired
    AddServer addServer;
    @RequestMapping("/add")
    public int add(int a, int b) {
        return addServer.add(a,b );
    }
    @RequestMapping("/person")
    public Person getPerson() {
        Person p = new Person("谷世羿", 33, "M", 30000);
        return p;
    }
}
