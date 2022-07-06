package com.batch.utils;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ManifestFileParser {
    public static String readFileAsString(String file) throws Exception {
        return new String(Files.readAllBytes(Paths.get(file)));
    }


    public static JsonObject getManifestFile(String jsonFilePath) {
        try {
            String json = readFileAsString(jsonFilePath);
            JsonObject json_object = new JsonParser().parse(json).getAsJsonObject();
            return json_object;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static JsonObject batchConfigDetails(String bucketName,String manifestFile) throws IOException {
        AmazonS3 s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider());

        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, manifestFile));


        String data = displayTextInputStream(fullObject.getObjectContent());
        //System.out.println(data);
        JsonObject jsonObject = convertingToJsonObject(data);
        //ystem.out.println(jsonObject.get(query));
        //return jsonObject.get(query);
        return jsonObject;

    }
    public static JsonObject convertingToJsonObject(String jsonFilePath) {
        try {
            JsonObject json_object = new JsonParser().parse(jsonFilePath).getAsJsonObject();
            return json_object;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    public static String displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuffer sb = new StringBuffer();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();

    }



    public static ManifestResponse getManifestResponse(JsonObject jsonObject) {
        ManifestResponse config = new ManifestResponse();
        config.setComponent(jsonObject.get("component").getAsString());
        config.setPilotId(jsonObject.get("pilotId").getAsInt());
        config.setWorkerInstanceCount(jsonObject.getAsJsonObject("emrParams").get("workerInstanceCount").getAsInt());
        config.setDiskSizeInGb(jsonObject.getAsJsonObject("emrParams").get("diskSizeInGb").getAsInt());
        config.setVolumePerInstance(jsonObject.getAsJsonObject("emrParams").get("volumePerInstance").getAsInt());
        config.setSkipSucceededTasksOnRetry(jsonObject.getAsJsonObject("batchSchedulingConfig").get("skipSucceededTasksOnRetry").getAsBoolean());
        config.setNextBatchDependentOnPrev(jsonObject.getAsJsonObject("batchSchedulingConfig").get("isNextBatchDependentOnPrev").getAsBoolean());
        config.setParallelBatchesIfIndependent(jsonObject.getAsJsonObject("batchSchedulingConfig").get("parallelBatchesIfIndependent").getAsInt());
        config.setMaxTries(jsonObject.getAsJsonObject("batchSchedulingConfig").get("maxTries").getAsInt());
        config.setComponent(jsonObject.getAsJsonObject("batchSchedulingConfig").get("dagId").getAsString());
        //config.setbatchId(String.valueOf(UUID.fromString(jsonObject.get("batchId").getAsString())));
        config.setbatchId(jsonObject.get("batchId").getAsString());
        config.setbatchCreationType(jsonObject.get("batchCreationType").getAsString());
        config.setbatchCreationTime(jsonObject.get("batchCreationTime").getAsString());
        config.setlatestObjectKey(jsonObject.get("latestObjectKey").getAsString());
        config.setBatchObjects((jsonObject.get("batchObjects").getAsJsonArray()));
        return config;
    }
}

/*
for converting json response to jsonarray
JSONArray arr = new JSONArray(yourJSONresponse);
List<String> list = new ArrayList<String>();
for(int i = 0; i < arr.length(); i++){
    list.add(arr.getJSONObject(i).getString("name"));
}
 */



