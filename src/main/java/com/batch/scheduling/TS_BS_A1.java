package com.batch.scheduling;
import com.amazonaws.services.s3.AmazonS3Client;


import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import com.batch.scheduling.TestCaseUtility;
import java.io.IOException;
import java.util.Calendar;
import java.util.logging.Logger;
@Test()
public class TS_BS_A1 {
    public void TS_BS_A1_T1(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.expectedBatchesTest(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_A1_T1 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_A1_T1 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_A1_T2(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.SanityCheckForBatchStepDetailsTable(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_A1_T2 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_A1_T2 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_A1_T3(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.checkAirflowStatusoOfDAGTest(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_A1_T3 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_A1_T3 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_A1_T4(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.checkAirflowStatusoOfDAGRunTest(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_A1_T4 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_A1_T4 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
    public void TS_BS_A1_T5(JsonObject batchConfig)
    {
        TestCaseUtility testCaseUtility=new TestCaseUtility();
        int count=testCaseUtility.checkAirflowDAGResponseValues(batchConfig);
        boolean test_case_pass=true;
        if(count>0)
        {
            test_case_pass=false;
            Reporter.log("TS_BS_A1_T5 failed",true);
        }
        else
        {
            Reporter.log("TS_BS_A1_T5 succeeded",true);
        }
        Assert.assertTrue(test_case_pass);
    }
}
