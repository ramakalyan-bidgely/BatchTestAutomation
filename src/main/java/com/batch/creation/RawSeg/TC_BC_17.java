package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.logging.Logger;

public class TC_BC_17 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test()
    void validate() throws IOException{
        //we need to modify one object which is considered for batch creation
        //This object should be considered for batch creation because the uploaded time is changed
        AmazonS3URI DEST_URI = new AmazonS3URI("s3://bidgely-adhoc-batch-qa/kalyan/ETE_RAW/10061/2022/06/");
        String Dir = " C:\\Users\\BizAct-64\\Downloads\\testcases_Summary.txt";
        List<File> fileName= S3FileTransferHandler.ListofFiles(Dir);
        // a file with proper naming convention is given to transfer files
        long DataAccumulatedSize = S3FileTransferHandler.TransferFiles(DEST_URI,Dir);
        InputConfigParser ConfigParser = new InputConfigParser();
        String jsonFilePath = "s3://bidgely-adhoc-dev/10061/rawingestion/raw_batch_config.json";
        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);
        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();
        // JsonObject value =InputConfigParser.getBatchInputs(batchConfig.get("batchConfigs").getAsString());
        //System.out.println(value);

        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);


        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String manifest_prefix = bc.getPrefix();
        Long dataSizeInbytes = bc.getDataSizeInBytes();
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        for(String str: GeneratedBatches){
            JsonObject jsonObject= ManifestFileParser.getManifestDetails(s3Bucket, str);
            JsonArray batchObjects=(jsonObject.get("batchObjects").getAsJsonArray());
            //Now we need to verify whether the files are there in the batchObjects or not.

        }
        Assert.assertEquals(issueCount,0);




    }
}
