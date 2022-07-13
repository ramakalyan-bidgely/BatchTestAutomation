package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.creation.ValidateManifestFile;
import com.batch.utils.*;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;

public class TC_BC_17 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());

    @Test()
    @Parameters({"batchConfigPath"})
    void validate(String batchConfigPath) throws IOException, InterruptedException {

        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);

        //we need to modify one object which is considered for batch creation
        //This object should be considered for batch creation because the uploaded time is changed
     /*   AmazonS3URI DEST_URI = new AmazonS3URI("s3://bidgely-adhoc-batch-qa/kalyan/ETE_RAW/10061/2022/06/");
        String Dir = "D:\\TEST DATA\\TC_BC_15\\DATA_FILES";
        String fileName= S3FileTransferHandler.ListofFiles(Dir).get(0).toString();
        // a file with proper naming convention is given to transfer files
        long DataAccumulatedSize = S3FileTransferHandler.TransferFiles(DEST_URI,Dir);*/
        JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);


        InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());
        // JsonObject value =InputConfigParser.getBatchInputs(batchConfig.get("batchConfigs").getAsString());
        //Reporter.log(value,true);


        String s3Prefix = "s3://";
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix(); long intervalInSec= bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();
        Long dataSizeInbytes = bc.getDataSizeInBytes();
        Integer maxLookUpDays = bc.getMaxLookUpDays();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";
        ArrayList<String> list = new ArrayList<String>();
        list.add("DATA_FILES/");
        list.add("DATA_FILES_MODIFIED/");

        for (String name : list) {
            Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
            VariableCollections.map.put("batch_creation_time", LatestBatchCreationTime);
            BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();

            Timestamp latest_modified_time = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);
            if (latest_modified_time == null) {
                Date now = new Date();
                Timestamp ts = new Timestamp(now.getTime());
                latest_modified_time = ts;
            }
            CharSequence fileName = null;

            String DEST = s3Prefix + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

            AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
            String SRC = s3Prefix + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName() + "/" + name;
            AmazonS3URI SRC_URI = new AmazonS3URI(SRC);

            long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
            Reporter.log("Data Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);

            //We can pass current automation execution date to prefix as Automation needs to test data from automation only
            Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(s3Bucket, BucketPrefix + "/" + dt, dataSizeInbytes, maxLookUpDays, latest_modified_time,LatestBatchCreationTime,intervalInSec);

            Reporter.log("Expected number of batches : " + ExpectedNoOfBatches, true);
            Thread.sleep(600000);
            int SIZE_BASED_CNT = 0;

            //Reporter.log("Latest Batch Creation Time: " + LatestBatchCreationTime,true);
            List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
            ArrayList<String> objectNames = S3FileTransferHandler.GetObjectKeys(DEST_URI);
            ArrayList<String> batchObjs = new ArrayList<>();
            for (String batchManifest : GeneratedBatches) {
                JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, batchManifest);
                if (jsonObject.get("batchCreationType").getAsString().equals("SIZE_BASED")) {
                    SIZE_BASED_CNT++;
                    Reporter.log("SIZE_BASED_CNT = " + SIZE_BASED_CNT, true);
                    Reporter.log("manifest file: " + batchManifest, true);

                    //passing batchConfig ,manifest Object details
                    ValidateManifestFile.ManifestFileValidation(s3Bucket, batchManifest, bc);
                }
                JsonArray batchObjects = (jsonObject.get("batchObjects").getAsJsonArray());
                for (JsonElement element : batchObjects) {
                    batchObjs.add(element.getAsString());
                }
            }
            issueCount += (SIZE_BASED_CNT == ExpectedNoOfBatches) ? 0 : 1;
            for (String value : objectNames) {
                if (!batchObjs.contains(value)) issueCount++;
            }
        }

        Assert.assertEquals(issueCount, 0);
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
    }
}
