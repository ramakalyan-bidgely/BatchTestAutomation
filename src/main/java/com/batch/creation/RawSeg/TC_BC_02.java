package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.BatchExecutionWatcher;
import com.batch.creation.DBEntryVerification;
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
    @Parameters({"batchConfigPath", "baseMinute"})
    public void validate(String batchConfigPath, String baseMinute) throws IOException, InterruptedException {

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

        Map<String, Long> ExpectedNoOfBatches = BatchCountValidator.getAccumulatedSize(pilotId, s3Bucket, BucketPrefix, dataSizeInbytes);


        BatchExecutionWatcher.bewatch(Integer.parseInt(baseMinute));


        int SIZE_BASED_CNT = 0;

        try {
            List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
            // now we need to verify the manifest files and check whether the object is present in it or not
            for (String str : GeneratedBatches) {
                JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, str);
                if (jsonObject.get("batchCreationType").getAsString().equals("SIZE_BASED")) {
                    SIZE_BASED_CNT++;
                    System.out.println("SIZE_BASED_CNT = " + SIZE_BASED_CNT);
                    System.out.println("manifest file: " + str);

                    issueCount += (SIZE_BASED_CNT == ExpectedNoOfBatches.get("ExpectedNumberOfBatches")) ? 0 : 1;
                } else {
                    issueCount++;
                }
                if (issueCount > 0) {
                    Reporter.log("Generated batches more than expected number of Batches");
                }
            }
        } catch (Throwable e) {
            // print stack trace
            e.printStackTrace();
        }

        Assert.assertEquals(issueCount, 0);
    }

}


