package com.batch;

import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;
import org.testng.collections.Lists;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;



import java.io.*;
import java.util.List;
import java.util.Properties;


/**
 * @author Rama Kalyan
 */
/*@Component
@PropertySource(value = "classpath:Batch.properties")*/
public class BatchFrameworkTestAutomation {

    static ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath*:batch-dao.xml");


/*

    @Value("${SuitePath}")
    private static String SuitePath;
    @Autowired
    private static Environment env;
*/


    public static void main(String[] args) throws IOException, ParseException {


    /*    Properties props = new Properties();
        try {
            props.load(BatchFrameworkTestAutomation.class.getResourceAsStream("Batch.properties"));
         }
        catch (IOException e) {
            e.printStackTrace();
        }
   */
       /* URL root = BatchFrameworkTestAutomation.class.getProtectionDomain().getCodeSource().getLocation();
        URL propertiesFile = new URL(root, "Batch.properties");
        Properties properties = new Properties();
        properties.load(propertiesFile.openStream());
*/




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

        System.out.println(SuitePath);

        suites.add(SuitePath);
        testng.setTestSuites(suites);
        testng.run();


    }


    // public static BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate) context.getBean("BatchJDBCTemplate");


}
