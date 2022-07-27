package com.batch.creation;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import static com.batch.creation.BatchCountValidator.amazons3Client;

@Test()
public class TC_BC_24 {

    private Integer issueCount = 0;


    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    @Parameters({"batchConfigPath", "triggerPoint"})
    void validate(JsonObject batchConfig) throws IOException, InterruptedException {
        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);

        //JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        //InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);

        int pilotId = bc.getPilotId();


        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        long intervalInSec = bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();

        Integer maxLookUpDays = bc.getMaxLookUpDays();

        Reporter.log("Max Lookup Days configured : " + maxLookUpDays, true);
        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";


        // get latestbatch Creation time
        Reporter.log("Getting latest batch creation time", true);
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);

        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();

        //Cleansing Old Data in aws and db

        int DelBatchDetails = batchJDBCTemplate.DelBatchDetails(pilotId, component);
        if (DelBatchDetails > 0) {
            Reporter.log("Records have been removed !", true);
        } else {
            Reporter.log("No records there to delete", true);
        }

        try {
            System.out.println("Deleting Objects in aws..");
            BatchCountValidator.delawsObjects(s3Bucket, BucketPrefix);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }

        // get latestbatch Creation time
        List<Map<String, Object>> latestObjectDetails = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);


        Timestamp latest_modified_time = (Timestamp) (latestObjectDetails.size() > 0 ? latestObjectDetails.get(0).get(LATEST_MODIFIED_TIME) : new Timestamp(new Date().getTime()));

        //Clearing up old data


        Thread.sleep(5000);

        List<String> LookUpDirectories = new ArrayList<>();

        c.setTime(new Date()); // Using today's date


        for (int i = 1; i <= 9; i++) {
            String dt = directoryStructure.equals("PartitionByDate") ? "date=" + new SimpleDateFormat("yyyy-MM-dd").format(c.getTime()) : new SimpleDateFormat("yyyy/MM/dd").format(c.getTime());
            if (i <= maxLookUpDays) {
                LookUpDirectories.add(dt);
            }
            String DEST = S3_PREFIX + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt + "/" + getClass().getSimpleName();
            AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
            String SRC = S3_PREFIX + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName() + "/dt" + i;
            AmazonS3URI SRC_URI = new AmazonS3URI(SRC);
            long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
            Reporter.log("Object Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);
            c.add(Calendar.DATE, -1);
        }
        //We can pass current automation execution date to prefix as Automation needs to test data from automation only
        Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(s3Bucket, BucketPrefix, dataSizeInbytes, maxLookUpDays, latest_modified_time, directoryStructure);

        Reporter.log("Expected number of batches : " + ExpectedNoOfBatches, true);


        BatchExecutionWatcher.bewatch(1);
        try {
            List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);          // now we need to verify the manifest files and check whether the object is present in it or not
            for (String batchManifest : GeneratedBatches) {
                JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, batchManifest);
                JsonArray batchObjects = jsonObject.get("batchObjects").getAsJsonArray();
                for (JsonElement batchObj : batchObjects) {
                    boolean ObjAvbl = false;
                    for (String Dir : LookUpDirectories) {
                        if (batchObj.toString().contains(Dir)) {
                            ObjAvbl = true;
                            continue;
                        }
                    }
                    if (!ObjAvbl) {
                        issueCount++;
                        Reporter.log("Object -> " + batchObj + " found beyond the LookUpDays Directories !", true);
                    }
                }
            }
        } catch (Throwable e) {
            // print stack trace
            e.printStackTrace();
        }

        Assert.assertEquals(issueCount, 0);
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
    }


}
