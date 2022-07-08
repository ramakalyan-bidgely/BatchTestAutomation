package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;


/**
 * @author Rama Kalyan
 */
@Test()
public class TC_BC_02 {

    //have to prepare data size of 300mb

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
    private Integer issueCount = 0;

    @Test()
    public void validate() throws IOException, InterruptedException {

        String jsonFilePath = "./raw_batch_config.json";


        InputConfigParser ConfigParser = new InputConfigParser();

        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);
        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();

        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);
        int pilotId = bc.getPilotId();

        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId + "/batchId";
        //Local to S3
        //String Dir = "D:\\TEST DATA\\TC_BC_02\\DATA_FILES";
        String DEST = "s3://bidgely-adhoc-batch-qa/TestAutomation/" + pilotId + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = "s3://bidgely-adhoc-batch-qa/TestData/" + pilotId + "/" + getClass().getSimpleName();
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);

        long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI, SRC_URI.getKey());
        System.out.println("Data Accumulated ......" + DataAccumulatedSize);
        long ExpectedNoOfBatches = DataAccumulatedSize / dataSizeInbytes;


        //Waiting for Batch Creation Service to be execute and complete
        Thread.sleep(10000);

        int SIZE_BASED_CNT = 0;

        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);

        System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);

        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        // now we need to verify the manifest files and check whether the object is present in it or not
        for (String str : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.batchConfigDetails(s3Bucket, str);
            if (jsonObject.get("batchCreationType").getAsString().equals("SIZE_BASED")) {
                SIZE_BASED_CNT++;
                issueCount += (SIZE_BASED_CNT == ExpectedNoOfBatches) ? 0 : 1;
            } else {
                issueCount++;
            }
            if (issueCount > 0) {
                Reporter.log("Generated batches more than expected number of Batches");
            }
        }

        Assert.assertEquals(issueCount, 0);
    }

}


