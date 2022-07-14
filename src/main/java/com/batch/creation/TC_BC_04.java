package com.batch.creation;

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
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
    @Parameters({"batchConfigPath", "triggerPoint"})
    void validate(String batchConfigPath, Integer triggerPoint) throws IOException, InterruptedException {
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


        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";

        dt = directoryStructure.equals("PartitionByDate") ? "date=" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) : new SimpleDateFormat("yyyy/MM/dd").format(new Date());
        String DEST = s3Prefix + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = s3Prefix + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName();
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);

        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        Reporter.log("Latest Batch Creation Time: " + LatestBatchCreationTime, true);

        long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
        Reporter.log("Data Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);


        Thread.sleep(600000);

        String ObjectNameKeyword = null;
        switch (dataSetType) {
            case "user_enrollment":
                ObjectNameKeyword = "USERENROLL";
                break;
            case "meter_enrollment":
                ObjectNameKeyword = "METERENROLL";
                break;
            case "raw_consumption_data":
                ObjectNameKeyword = "RAW";
                break;
            case "invoice":
                ObjectNameKeyword = "INVOICE";
                break;
            default:
                Reporter.log("Invalid Object Name Keyword", true);
        }


        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        // now we need to verify the manifest files and check whether the object is present or not
        for (String str : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, str);
            JsonArray batchObjects = (jsonObject.get("batchObjects").getAsJsonArray());
            for (JsonElement element : batchObjects) {
                if (!element.getAsString().contains(ObjectNameKeyword)) {
                    issueCount++;
                    Reporter.log("Object name does not contain specified keyword -> " + ObjectNameKeyword, true);
                } else {
                    Reporter.log("Object -> " + element + " contain keyword -> " + ObjectNameKeyword, true);
                }
            }
        }


        Assert.assertEquals(issueCount, 0);
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);

    }


}

