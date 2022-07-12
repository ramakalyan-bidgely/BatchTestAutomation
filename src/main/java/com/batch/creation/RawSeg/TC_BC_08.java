package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.BatchExecutionWatcher;
import com.batch.creation.DBEntryVerification;
import com.batch.creation.ValidateManifestFile;
import com.batch.utils.*;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;


@Test()
public class TC_BC_08 {

    //have to prepare data size of 300mb


    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
    private Integer issueCount = 0;

    @Test()
    @Parameters({"batchConfigPath", "triggerPoint"})
    public void validate(String batchConfigPath, Integer triggerPoint) throws IOException, InterruptedException {

        JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());

        int pilotId = bc.getPilotId();

        String s3Prefix = "s3://";
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String dataSetType = bc.getDatasetType();
        Integer maxLookUpDays = bc.getMaxLookUpDays();


        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";

        String DEST = s3Prefix + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = s3Prefix + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName();
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);


        // get latestbatch Creation time
        Reporter.log("Getting latest batch creation time", true);
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        VariableCollections.map.put("batch_creation_time", LatestBatchCreationTime);
        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();

        Timestamp latest_modified_time = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);
        if (latest_modified_time == null) {
            Date now = new Date();
            Timestamp ts = new Timestamp(now.getTime());
            latest_modified_time = ts;
        }

        long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
        Reporter.log("Data Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);


        //We can pass current automation execution date to prefix as Automation needs to test data from automation only
        Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(pilotId, component, s3Bucket, BucketPrefix + "/" + dt, dataSizeInbytes, maxLookUpDays, latest_modified_time);

        Reporter.log("Expected number of batches : " + ExpectedNoOfBatches, true);

        //BatchExecutionWatcher.bewatch(triggerPoint);
        Thread.sleep(600000);

        int SIZE_BASED_CNT = 0;
        try {
            List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
            // now we need to verify the manifest files and check whether the object is present in it or not
            ArrayList<String> objectNames = S3FileTransferHandler.GettingObjectsNames(DEST_URI);
            ArrayList<String> batchObjs = new ArrayList<>();
            for (String batchManifest : GeneratedBatches) {
                JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, batchManifest);
                if (jsonObject.get("batchCreationType").getAsString().equals("SIZE_BASED")) {
                    SIZE_BASED_CNT++;
                    Reporter.log("SIZE_BASED_CNT = " + SIZE_BASED_CNT, true);
                    Reporter.log("manifest file: " + batchManifest, true);

                    //passing batchConfig ,manifest Object details
                    ValidateManifestFile.ManifestFileValidation(s3Bucket, batchManifest, bc);
                } else {
                    issueCount++;
                }
                JsonArray batchObjects = (jsonObject.get("batchObjects").getAsJsonArray());
                for (JsonElement element : batchObjects) {
                    batchObjs.add(element.getAsString());
                }
                if (issueCount > 0) {
                    Reporter.log("Generated batches more than expected number of Batches", true);
                }
            }
            issueCount += (SIZE_BASED_CNT == ExpectedNoOfBatches) ? 0 : 1;
            for (String value : objectNames) {
                if (!batchObjs.contains(value)) issueCount++;
            }
        } catch (Throwable e) {
            // print stack trace
            e.printStackTrace();
        }

        Assert.assertEquals(issueCount, 0);
    }

}


