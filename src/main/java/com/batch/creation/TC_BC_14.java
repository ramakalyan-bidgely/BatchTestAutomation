package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.BatchExecutionWatcher;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.*;

public class TC_BC_14 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    @Parameters({"batchConfigPath", "triggerPoint"})
    void validate(JsonObject batchConfig) throws IOException, InterruptedException {
        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);

        //JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        //InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);

        String s3Prefix = "s3://";
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        String dataSetType = bc.getDatasetType();
        long intervalInSec = bc.getIntervalInSec();
        Integer maxLookUpDays = bc.getMaxLookUpDays();
        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);

        Reporter.log("Latest Batch Creation Time: " + LatestBatchCreationTime, true);
        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();
          List<Map<String, Object>> latestObjectDetails = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);

        Timestamp latest_modified_time = (Timestamp) (latestObjectDetails.size() > 0 ? latestObjectDetails.get(0).get(LATEST_MODIFIED_TIME) : new Timestamp(new Date().getTime()));
        String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
        dt = directoryStructure.equals("PartitionByDate") ? "date=" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) : new SimpleDateFormat("yyyy/MM/dd").format(new Date());
        String DEST = S3_PREFIX + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = S3_PREFIX + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName();
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);


        long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
        Reporter.log("Data Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);

        Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(s3Bucket, BucketPrefix + "/" + dt, dataSizeInbytes, maxLookUpDays, latest_modified_time, LatestBatchCreationTime, intervalInSec);

        Reporter.log("Expected number of batches : " + ExpectedNoOfBatches, true);

        Thread.sleep(600000);


        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        List<String> list = new ArrayList<String>();

        for (String batchManifest : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, batchManifest);
            JsonArray batchObjects = jsonObject.get("batchObjects").getAsJsonArray();
            for (JsonElement arrayValues : batchObjects) {
                String batchObj = arrayValues.getAsString();
                if (!list.contains(batchObj)) {
                    list.add(batchObj);
                } else {
                    issueCount++;
                    Reporter.log("Duplicate Object found -> " + batchObj, true);
                }
            }
        }
        if (issueCount == 0) {
            Reporter.log("No Duplicates found", true);
        }
        Assert.assertEquals(issueCount, 0);
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
    }
}