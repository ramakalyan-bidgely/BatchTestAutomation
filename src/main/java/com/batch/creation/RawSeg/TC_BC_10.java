package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.BatchExecutionWatcher;
import com.batch.creation.DBEntryVerification;
import com.batch.creation.ValidateManifestFile;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;



@Test()
public class TC_BC_10 {


    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
    private Integer issueCount = 0;

    @Test()
    @Parameters({"batchConfigPath", "baseMinute"})
    public void validate(String batchConfigPath, Integer baseMinute) throws IOException, InterruptedException {

        JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);


        InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());


        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";
        //Local to S3
        //String Dir = "D:\\TEST DATA\\TC_BC_02\\DATA_FILES";
        String DEST = "s3://bidgely-adhoc-batch-qa/TestAutomation/" + pilotId + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = "s3://bidgely-adhoc-batch-qa/TestData/" + pilotId + "/" + getClass().getSimpleName();
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);


        // get latestbatch Creation time
        System.out.println("Getting latest batch creation time");
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);

        long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
        System.out.println("Data Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize);


        //We can pass current automation execution date to prefix as Automation needs to test data from automation only
        Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(pilotId, component, s3Bucket, BucketPrefix + "/" + dt, dataSizeInbytes);

        System.out.println("Expected number of batches : " + ExpectedNoOfBatches);

        //BatchExecutionWatcher.bewatch(baseMinute);
        Thread.sleep(600000);

        int TIME_BASED_CNT = 0;
        try {
            List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
            // now we need to verify the manifest files and check whether the object is present in it or not
            for (String batchManifest : GeneratedBatches) {
                JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, batchManifest);
                if (jsonObject.get("batchCreationType").getAsString().equals("TIME_BASED")) {
                    TIME_BASED_CNT++;
                    System.out.println("TIME_BASED_CNT = " + TIME_BASED_CNT);
                    System.out.println("manifest file: " + batchManifest);

                    //passing batchConfig ,manifest Object details
                    ValidateManifestFile.ManifestFileValidation(s3Bucket, batchManifest, bc);


                } else {
                    issueCount++;
                }
                if (issueCount > 0) {
                    Reporter.log("Generated batches more than expected number of Batches");
                }
            }
            issueCount += (TIME_BASED_CNT == ExpectedNoOfBatches) ? 0 : 1;
        } catch (Throwable e) {
            // print stack trace
            e.printStackTrace();
        }

        Assert.assertEquals(issueCount, 0);
    }

}


