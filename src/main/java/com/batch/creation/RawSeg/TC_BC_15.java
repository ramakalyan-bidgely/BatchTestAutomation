package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

public class TC_BC_15 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());

    @Test()
    void validate() throws IOException, InterruptedException {
        //we need to modify one object which is considered for batch creation
        //This object should be considered for batch creation because the uploaded time is changed
     /*   AmazonS3URI DEST_URI = new AmazonS3URI("s3://bidgely-adhoc-batch-qa/kalyan/ETE_RAW/10061/2022/06/");
        String Dir = "D:\\TEST DATA\\TC_BC_15\\DATA_FILES";
        String fileName= S3FileTransferHandler.ListofFiles(Dir).get(0).toString();
        // a file with proper naming convention is given to transfer files
        long DataAccumulatedSize = S3FileTransferHandler.TransferFiles(DEST_URI,Dir);*/
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
        String BucketPrefix = bc.getPrefix();
        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId + "/batchId";
        ArrayList<String> list = new ArrayList<String>();
        list.add("DATA_FILES/");
        list.add("DATA_FILES_MODIFIED/");

        for(String name : list)
        {
            Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
            CharSequence fileName = null;
        //Local to S3
        //String Dir = "D:\\TEST DATA\\TC_BC_02\\DATA_FILES";
        String DEST = "s3://bidgely-adhoc-batch-qa/TestAutomation/" + pilotId + "/" + dt + "/" + getClass().getSimpleName() + "/";
        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);
        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = "s3://bidgely-adhoc-batch-qa/TestData/" + pilotId +"/" + getClass().getSimpleName() + "/"+name;
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);
        Thread.sleep(10000);


        //System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        ArrayList<String> objectNames = S3FileTransferHandler.GettingObjectsNames(DEST_URI);
        ArrayList<String> batchObjs =new ArrayList<>();
        for(String str: GeneratedBatches){
            JsonObject jsonObject= ManifestFileParser.getManifestDetails(s3Bucket, str);
            JsonArray batchObjects=(jsonObject.get("batchObjects").getAsJsonArray());
                for(JsonElement element:batchObjects){
                    batchObjs.add(element.getAsString());
                }
            }
        for(String value:objectNames){
            if(!batchObjs.contains(value)) issueCount++;


        }
        }

        Assert.assertEquals(issueCount,1);
    }
}
