package com.batch;

import com.batch.utils.sql.batch.BatchJDBCTemplate;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.collections.Lists;

import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author Rama Kalyan
 */

public class BatchFrameworkTestAutomation {

    static ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath*:batch-dao.xml");
    public static void main(String[] args) throws IOException, ParseException {

/*
        Properties props = new Properties();
        try {
            props.load(BatchFrameworkTestAutomation.class.getResourceAsStream("Batch.properties"));
         }
        catch (IOException e) {
            e.printStackTrace();
        }
   */
        URL root = BatchFrameworkTestAutomation.class.getProtectionDomain().getCodeSource().getLocation();
        URL propertiesFile = new URL(root, "Batch.properties");
        Properties properties = new Properties();
        properties.load(propertiesFile.openStream());





     /* //Reading Properties file
       FileReader PropReader = new FileReader("Batch.properties");

        Properties props = new Properties();
        try {
            props.load(PropReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/

    /*  String batchInputConfig = props.getProperty("batchInputConfig");
        JSONParser jsonParser = new JSONParser();
        FileReader jsonreader = new FileReader(batchInputConfig);
        Object obj = jsonParser.parse(jsonreader);
        JSONObject jobj = (JSONObject) obj;*/



        TestListenerAdapter tla = new TestListenerAdapter();
        TestNG testng = new TestNG();
        List<String> suites = Lists.newArrayList();

/*
        try (InputStream inputStream = BatchFrameworkTestAutomation.class.getResourceAsStream("/testng.xml"); BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String contents = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }*/
        String SuitePath = properties.getProperty("SuitePath");
        System.out.println(SuitePath);
        suites.add(SuitePath);
        testng.setTestSuites(suites);
        testng.run();


    }
    // public static BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate) context.getBean("BatchJDBCTemplate");


}
