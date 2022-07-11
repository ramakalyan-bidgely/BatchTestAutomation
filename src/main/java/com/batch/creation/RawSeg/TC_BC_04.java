package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.BatchExecutionWatcher;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;

@Test
public class TC_BC_04 {

    private Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());

    @Test()
    @Parameters({"batchConfigPath", "baseMinute"})
    void validate(String batchConfigPath, Integer baseMinute) throws IOException, InterruptedException {

        JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());

        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";

        //Local to S3
        //String Dir = "D:\\TEST DATA\\TC_BC_02\\DATA_FILES";
        String DEST = "s3://bidgely-adhoc-batch-qa/TestAutomation/" + pilotId + "/" + dt + "/" + getClass().getSimpleName();
        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);
        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = "s3://bidgely-adhoc-batch-qa/TestData/" + pilotId + "/" + getClass().getSimpleName();
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);

        long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);

        //BatchExecutionWatcher.bewatch(baseMinute);
        Thread.sleep(600000);

        String DataSetKeyWord = "RAW";

        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        // now we need to verify the manifest files and check whether the object is present or not
        for (String str : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, str);
            JsonArray batchObjects = (jsonObject.get("batchObjects").getAsJsonArray());
            for (JsonElement element : batchObjects) {
                if (!element.getAsString().contains(DataSetKeyWord)) {
                    issueCount++;
                    System.out.println("Object name does not contain specified keyword -> " + DataSetKeyWord);
                }
            }
        }


        Assert.assertEquals(issueCount, 0);

    }


}

