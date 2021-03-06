package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.creation.ValidateManifestFile;
import com.batch.utils.*;
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

public class TC_BC_21 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());

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
        String BucketPrefix = bc.getPrefix(); String directoryStructure = bc.getDirectoryStructure();
        long intervalInSec = bc.getIntervalInSec();
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
            CharSequence fileName = null;
            BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();

            List<Map<String, Object>> latestObjectDetails = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);
            Timestamp latest_modified_time = (Timestamp) latestObjectDetails.get(0).get(LATEST_MODIFIED_TIME);
            if (latest_modified_time == null) {
                Date now = new Date();
                Timestamp ts = new Timestamp(now.getTime());
                latest_modified_time = ts;
            }

            dt = directoryStructure.equals("PartitionByDate") ? "date=" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) : new SimpleDateFormat("yyyy/MM/dd").format(new Date());
            String DEST = S3_PREFIX + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

            AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
            String SRC = S3_PREFIX + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName() + "/" + name;
            AmazonS3URI SRC_URI = new AmazonS3URI(SRC);

            long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
            Reporter.log("Data Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);

            //We can pass current automation execution date to prefix as Automation needs to test data from automation only
            Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(s3Bucket, BucketPrefix + "/" + dt, dataSizeInbytes, maxLookUpDays, latest_modified_time, directoryStructure);

            Reporter.log("Expected number of batches : " + ExpectedNoOfBatches, true);
            Thread.sleep(600000);
            int TIME_BASED_CNT = 0;

            //Reporter.log("Latest Batch Creation Time: " + LatestBatchCreationTime,true);
            List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
            ArrayList<String> objectNames = S3FileTransferHandler.GetObjectKeys(DEST_URI);
            ArrayList<String> batchObjs = new ArrayList<>();
            for (String batchManifest : GeneratedBatches) {
                JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, batchManifest);
                if (jsonObject.get("batchCreationType").getAsString().equals("TIME_BASED")) {
                    TIME_BASED_CNT++;
                    Reporter.log("TIME_BASED_CNT = " + TIME_BASED_CNT, true);
                    

                    //passing batchConfig ,manifest Object details
                    ValidateManifestFile.ManifestFileValidation(s3Bucket, batchManifest, bc);
                }
                JsonArray batchObjects = (jsonObject.get("batchObjects").getAsJsonArray());
                for (JsonElement element : batchObjects) {
                    batchObjs.add(element.getAsString());
                }
            }
            issueCount += (TIME_BASED_CNT == ExpectedNoOfBatches) ? 0 : 1;
            for (String value : objectNames) {
                if (!batchObjs.contains(value)) issueCount++;
            }
        }

        Assert.assertEquals(issueCount, 0);
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
    }
}
