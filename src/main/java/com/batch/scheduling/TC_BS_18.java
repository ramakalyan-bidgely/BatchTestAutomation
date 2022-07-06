package com.batch.scheduling;

import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

@Test()
public class TC_BS_18 {
    @Test(dataProvider = "input-data-provider",dataProviderClass= MainDataProvider.class)
    public void validate(JsonObject data){
        InputConfig config=InputConfigParser.getInputConfig(data);
        Integer workerInstanceCount=config.getWorkerInstanceCount();
        Long dataSizeInBytes=config.getDataSizeInBytes();
        Integer volumePerInstance=config.getVolumePerInstance();
        if(workerInstanceCount<=0){
            Reporter.log("workerInstanceCount  is less than zero");
        }
        if(workerInstanceCount<=0){
            Reporter.log("dataSizeInBytes  is less than zero");
        }
        if(workerInstanceCount<=0){
            Reporter.log("volumePerInstance  is less than zero");
        }
        Boolean allGreater=(workerInstanceCount>0)&(dataSizeInBytes>0)&(volumePerInstance>0);
        Assert.assertTrue(allGreater);
    }


}
