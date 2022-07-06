package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.google.gson.JsonObject;
import org.testng.annotations.Test;

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
    private final Integer issueCount = 0;

    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());

    @Test()
    public void validate() throws IOException {

        String jsonFilePath = "s3://bidgely-adhoc-dev/10061/rawingestion/raw_batch_config.json";

        AmazonS3URI DEST_URI = new AmazonS3URI("s3://bidgely-adhoc-batch-qa/TestAutomation/10061/" + dt + "/" + getClass().getSimpleName() + "/");
        String Dir = "D:\\TEST DATA\\TC_BC_02\\DATA_FILES";
        long DataAccumulatedSize = S3FileTransferHandler.TransferFiles(DEST_URI, Dir);
        InputConfigParser ConfigParser = new InputConfigParser();

        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);
        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();


        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);
        int pilotId = bc.getPilotId();

        //new push
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String manifest_prefix = bc.getPrefix();
        Long dataSizeInbytes = bc.getDataSizeInBytes();

        Long ExpectedNumberOfBatches = DataAccumulatedSize / dataSizeInbytes;

        int SIZE_BASED_CNT = 0;

        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        // now we need to verify the manifest files and check whether the object is present in it or not
        for (String str : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.batchConfigDetails(s3Bucket, str);
            if (jsonObject.get("batchCreationType").getAsString().equals("SIZE_BASED")) SIZE_BASED_CNT++;
        }


        // Assert.assertEquals(Optional.ofNullable(issueCount), 1);


    }

}


