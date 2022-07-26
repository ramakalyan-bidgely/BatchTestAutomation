package com.batch.utils.sql.batch;

import com.batch.utils.InputConfigParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.testng.annotations.DataProvider;


public class MainDataProvider {
    @DataProvider(name = "input-data-provider", parallel = true)
    public Object[][] dp() {

        String ConfigFileJsonPath[] = {"raw_batch_config.json"};
        Integer i = 0;

        InputConfigParser x = new InputConfigParser();
        JsonObject jsonobj = InputConfigParser.getBatchConfig(ConfigFileJsonPath[i]);
        JsonArray jsonArr = jsonobj.getAsJsonArray("batchConfigs");
        int arrSize = jsonArr.size();
        Object[][] returnValue = new Object[arrSize][1];
        int index = 0;
        for (Object[] each : returnValue) {
            each[0] = jsonArr.get(index++).getAsJsonObject();
        }
        return returnValue;
    }
    @DataProvider(name = "A1-data-provider")
    public Object[][] dpA1() {

        String ConfigFileJsonPath[] = {"user_batch_config.json"};
        Integer i = 0;

        InputConfigParser x = new InputConfigParser();
        JsonObject jsonobj = InputConfigParser.getBatchConfig(ConfigFileJsonPath[i]);
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

//    @DataProvider(name = "input-data-provider", parallel = true)
//    public Object[][] dp() {
//
//        String ConfigFileJsonPath[] = {"user_batch_config.json", "raw_batch_config.json"};
//        Integer i = 0;
//        for (i = 0; i < ConfigFileJsonPath.length; i++) {
//            InputConfigParser x = new InputConfigParser();
//            JsonObject jsonobj = InputConfigParser.getBatchConfig(ConfigFileJsonPath[i]);
//            JsonArray jsonArr = jsonobj.getAsJsonArray("batchConfigs");
//            int arrSize = jsonArr.size();
//            Object[][] returnValue = new Object[arrSize][1];
//            int index = 0;
//            for (Object[] each : returnValue) {
//                each[0] = jsonArr.get(index++).getAsJsonObject();
//            }
//            return returnValue;
//        }
//
//    }

