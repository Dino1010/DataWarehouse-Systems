package com.daniel51.cd.data_service.controller;/*
 @author Daniel51
 @DESCRIPTION add a,b
 @create 2019/7/22
*/

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AddController {
    @RequestMapping("/add")
    public int add(int a, int b) {
        return 2;
    }
    
}
