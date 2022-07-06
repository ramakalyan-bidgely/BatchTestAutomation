package com.batch.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.nio.file.Files;
import java.nio.file.Paths;

public class InputConfigParser {
    public static String readFileAsString(String file) throws Exception
    {
        return new String(Files.readAllBytes(Paths.get(file)));
    }


    public static JsonObject getBatchConfig(String jsonFilePath){
        try {
            String json = readFileAsString(jsonFilePath);
            JsonObject json_object = new JsonParser().parse(json).getAsJsonObject();
            return json_object;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


    public static InputConfig getInputConfig(JsonObject jsonObject)
    {
        InputConfig config=new InputConfig();
        config.setComponent(jsonObject.get("component").getAsString());
        config.setPilotId(jsonObject.get("pilotId").getAsInt());
        config.setWorkerInstanceCount(jsonObject.getAsJsonObject("emrParams").get("workerInstanceCount").getAsInt());
        config.setDiskSizeInGb(jsonObject.getAsJsonObject("emrParams").get("diskSizeInGb").getAsInt());
        config.setVolumePerInstance(jsonObject.getAsJsonObject("emrParams").get("volumePerInstance").getAsInt());
        config.setBucket(jsonObject.getAsJsonObject("batchCreationConfig").get("bucket").getAsString());
        config.setPrefix(jsonObject.getAsJsonObject("batchCreationConfig").get("prefix").getAsString());
        config.setDirectoryStructure(jsonObject.getAsJsonObject("batchCreationConfig").get("directoryStructure").getAsString());
        config.setDatasetType(jsonObject.getAsJsonObject("batchCreationConfig").get("datasetType").getAsString());
        config.setDataFormat(jsonObject.getAsJsonObject("batchCreationConfig").get("dataFormat").getAsString());
        config.setCompressionFormat(jsonObject.getAsJsonObject("batchCreationConfig").get("compressionFormat").getAsString());
        config.setMaxLookUpDays(jsonObject.getAsJsonObject("batchCreationConfig").get("maxLookUpDays").getAsInt());
        config.setDataSizeInBytes(jsonObject.getAsJsonObject("batchCreationConfig").get("dataSizeInBytes").getAsInt());
        config.setIntervalInSec(jsonObject.getAsJsonObject("batchCreationConfig").get("intervalInSec").getAsInt());
        config.setSkipSucceededTasksOnRetry(jsonObject.getAsJsonObject("batchSchedulingConfig").get("skipSucceededTasksOnRetry").getAsBoolean());
        config.setNextBatchDependentOnPrev(jsonObject.getAsJsonObject("batchSchedulingConfig").get("isNextBatchDependentOnPrev").getAsBoolean());
        config.setParallelBatchesIfIndependent(jsonObject.getAsJsonObject("batchSchedulingConfig").get("parallelBatchesIfIndependent").getAsInt());
        config.setMaxTries(jsonObject.getAsJsonObject("batchSchedulingConfig").get("maxTries").getAsInt());
        config.setComponent(jsonObject.getAsJsonObject("batchSchedulingConfig").get("dagId").getAsString());
        return config;


    }

}




