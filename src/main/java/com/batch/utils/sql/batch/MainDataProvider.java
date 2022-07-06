package com.batch.utils.sql.batch;

import com.batch.utils.InputConfigParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.testng.annotations.DataProvider;


public class MainDataProvider {
    @DataProvider(name = "input-data-provider")
    public Object[][] dp() {

        String ConfigFileJsonPath = "C:\\Users\\BizAct-64\\Desktop\\BatchSchedulingCases\\src\\main\\resources\\input_config.json";
        InputConfigParser x = new InputConfigParser();
        JsonObject jsonobj = InputConfigParser.getBatchConfig(ConfigFileJsonPath);
        JsonArray jsonArr = jsonobj.getAsJsonArray("batchConfigs");
        int arrSize = jsonArr.size();
        Object[][] returnValue = new Object[arrSize][1];
        int index = 0;
        for (Object[] each : returnValue) {
            each[0] = jsonArr.get(index++).getAsJsonObject();
        }
        return returnValue;
    }

    @DataProvider(name = "manifest-data-provider")
    public Object[][] mdp() {
        String ConfigFileJsonPath = "C:\\Users\\BizAct-64\\Desktop\\BatchSchedulingCases\\src\\main\\resources\\input_config.json";
        InputConfigParser x = new InputConfigParser();
        JsonObject jsonobj = InputConfigParser.getBatchConfig(ConfigFileJsonPath);
        JsonArray jsonArr = jsonobj.getAsJsonArray("batchConfigs");
        int arrSize = jsonArr.size();

        Object[][] returnValue = new Object[arrSize][1];

        int index = 0;
        for (Object[] each : returnValue) {
            each[0] = jsonArr.get(index++).getAsJsonObject();
        }
        return returnValue;
    }
}
