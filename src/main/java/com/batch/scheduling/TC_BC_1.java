package com.batch.scheduling;

import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.google.gson.JsonObject;
public class TC_BC_1 {
    String config_file_path = "resources/input_config.json";
    public InputConfigParser x = new InputConfigParser();

    JsonObject jsonobj = x.getBatchConfig(config_file_path);
    JsonObject b=jsonobj.getAsJsonArray("batchConfigs").get(0).getAsJsonObject();
    InputConfig a=x.getInputConfig(b);

}