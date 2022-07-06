package com.batch.creation.RawSeg;

import com.batch.creation.BatchCountValidator;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class TC_BC_14 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test()
    void validate() throws IOException {
        //doubt
        //we need to go through all the generated manifest files and check whether any object is repeating or not

        InputConfigParser ConfigParser = new InputConfigParser();
        String jsonFilePath = "s3://bidgely-adhoc-dev/10061/rawingestion/raw_batch_config.json";
        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);
        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();
        List<String> list = new ArrayList<String>();
        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String manifest_prefix = bc.getPrefix();
        List<String> GeneratedBatches= BatchCountValidator.getBatchManifestFiles(pilotId, component, s3Bucket, manifest_prefix);
        for(String str: GeneratedBatches){
            JsonObject jsonObject= ManifestFileParser.batchConfigDetails(s3Bucket,str);
            JsonArray batchObjects = jsonObject.get("batchObjects").getAsJsonArray();
            for(JsonElement arrayValues:batchObjects){
                String value = arrayValues.getAsString();
                //System.out.println(value);
                if(!list.contains(value)){
                    list.add(value);
                    //System.out.println(value);
                }

                else {issueCount++;
                    System.out.println(value);}

                //System.out.println(str.getAsString().contains("RAW_D_15_S_202201310641_1202"));
            }
        }
        Assert.assertEquals(issueCount,0);
        }
    }