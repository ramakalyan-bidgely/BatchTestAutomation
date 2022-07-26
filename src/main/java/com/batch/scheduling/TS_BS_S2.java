package com.batch.scheduling;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;
import org.testng.Reporter;


import java.util.List;
import java.util.logging.Logger;


public class TS_BS_S2 {
    public void TS_BS_S2_T1(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.expectedBatchesTest(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_S2_T1 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_S2_T1 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_S2_T2(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.SanityCheckForBatchStepDetailsTable(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_S2_T2 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_S2_T2 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_S2_T3(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.checkAirflowStatusoOfDAGTest(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_S2_T3 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_S2_T3 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_S2_T4(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.checkAirflowStatusoOfDAGRunTest(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_S2_T4 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_S2_T4 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_S2_T5(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.checkAirflowDAGResponseValues(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_S2_T5 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_S2_T5 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
}
