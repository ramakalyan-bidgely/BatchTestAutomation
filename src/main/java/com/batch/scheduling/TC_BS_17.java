package com.batch.scheduling;

import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

@Test()
public class TC_BS_17 {
    @Test(dataProvider = "input-data-provider",dataProviderClass= MainDataProvider.class)
    public void validate(JsonObject data) {
        InputConfig config = InputConfigParser.getInputConfig(data);
        Boolean anyDateTyepMismatch=false;


        if(((Object) config.getMaxTries()).getClass().getName()!="java.lang.Integer"){
            anyDateTyepMismatch=true;
            Reporter.log("MaxTries is not an Integer");
        }

        if(config.getDagId()!=null){
        if(((Object) config.getDagId()).getClass().getName()!="java.lang.String"){
            anyDateTyepMismatch=true;
            Reporter.log("DagId is not a String");
        }}
        else{
            anyDateTyepMismatch=true;
            Reporter.log("DagId is a Null Value");

        }
        if(((Object) config.getParallelBatchesIfIndependent()).getClass().getName()!="java.lang.Integer"){
            anyDateTyepMismatch=true;
            Reporter.log("ParallelBatchesIfIndependent is not an Integer");
        }
        if(((Object) config.isSkipSucceededTasksOnRetry()).getClass().getName()!="java.lang.Boolean"){
            anyDateTyepMismatch=true;
            Reporter.log("SkipSucceededTasksOnRetry is not a Boolean");
        }
        if(((Object) config.isNextBatchDependentOnPrev()).getClass().getName()!="java.lang.Boolean"){
            anyDateTyepMismatch=true;
            Reporter.log("isNextBatchDependentOnPrev is not a Boolean");
        }
        Assert.assertFalse(anyDateTyepMismatch);

    }
}
