package com.batch;

import com.batch.utils.sql.batch.BatchJDBCTemplate;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.collections.Lists;


import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * @author Rama Kalyan
 */


public class BatchFrameworkTestAutomation {

    public static ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath*:batch-dao.xml");
    public static void main(String[] args) throws IOException, ParseException {

        //Reading Properties file
        FileReader PropReader = new FileReader(args[0]);
        Properties props = new Properties();
        try {
            props.load(PropReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        TestListenerAdapter tla = new TestListenerAdapter();
        TestNG testng = new TestNG();
        List<String> suites = Lists.newArrayList();
        String SuitePath = props.getProperty("SuitePath");
        suites.add(SuitePath);
        testng.setTestSuites(suites);
        testng.run();



    }


}
