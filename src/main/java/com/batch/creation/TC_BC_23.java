package com.batch.creation;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.BatchExecutionWatcher;
import com.batch.creation.DBEntryVerification;
import com.batch.creation.ValidateManifestFile;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
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

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;

@Test()
public class TC_BC_23 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());

    @Test()
    @Parameters("batchConfigPath")
    public void validate(String batchConfigPath) throws IOException, InterruptedException {


        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);

        JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());

        int pilotId = bc.getPilotId();

        String s3Prefix = "s3://";
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix(); 
	          String directoryStructure = bc.getDirectoryStructure(); 
	          long intervalInSec= bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();

        Integer maxLookUpDays = bc.getMaxLookUpDays();


        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";


        // get latestbatch Creation time
        Reporter.log("Getting latest batch creation time", true);
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);

        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();

        Timestamp latest_modified_time = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);
        if (latest_modified_time == null) {
            Date now = new Date();
            Timestamp ts = new Timestamp(now.getTime());
            latest_modified_time = ts;
        }

        //Clearing up old data
        AmazonS3Client amazonS3Client = new AmazonS3Client();
        try {
            amazonS3Client.deleteObject(s3Bucket, s3Prefix + "TestAutomation/" + pilotId + "/" + dataSetType);
            System.out.println("Deleted Objects");
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }

        Thread.sleep(10000);

        List<String> LookUpDirectories = new ArrayList<>();

        c.setTime(new Date()); // Using today's date


        SimpleDateFormat dt = new SimpleDateFormat("yyyy/MM/dd");
        for (int i = 1; i <= 9; i++) {
            if (i <= maxLookUpDays) {
                LookUpDirectories.add(dt.format(c.getTime()));
            }
            String DEST = s3Prefix + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt.format(c.getTime()) + "/" + getClass().getSimpleName();
            AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
            String SRC = s3Prefix + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName() + "/dt" + i;
            AmazonS3URI SRC_URI = new AmazonS3URI(SRC);
            long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
            Reporter.log("Object Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);
            c.add(Calendar.DATE, -1); // Adding 5 days
        }
        //We can pass current automation execution date to prefix as Automation needs to test data from automation only
       /* Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(s3Bucket, BucketPrefix, dataSizeInbytes, maxLookUpDays,latest_modified_time);

        Reporter.log("Expected number of batches : " + ExpectedNoOfBatches, true);*/

        Reporter.log("Waiting for batch creation service to complete .. ", true);
        Thread.sleep(600000);
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
