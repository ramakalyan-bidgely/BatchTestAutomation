package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

@Test
public class TC_BC_04 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private final Integer issueCount = 0;
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
    @Test()
    void validate() throws IOException {
        AmazonS3URI DEST_URI = new AmazonS3URI("s3://bidgely-adhoc-batch-qa/kalyan/ETE_RAW/10061/2022/06/");
        String Dir = "D:\\TEST DATA\\TC_BC_04\\DATA_FILES";
        String[] data = Dir.split("\\\\");
        String fileName = data[data.length - 1];


        // a file with proper naming convention is given to transfer files
        long DataAccumulatedSize = S3FileTransferHandler.TransferFiles(DEST_URI, Dir);
        int issueCount = 0;
        InputConfigParser ConfigParser = new InputConfigParser();
        String jsonFilePath = "s3://bidgely-adhoc-dev/10061/rawingestion/raw_batch_config.json";
        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);
        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();

        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId + "/batchId";

        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        // now we need to verify the manifest files and check whether the object is present or not
        for (String str : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.batchConfigDetails(s3Bucket, str);
            JsonArray batchObjects = (jsonObject.get("batchObjects").getAsJsonArray());
            for (JsonElement element : batchObjects) {
                if (element.getAsString().contains(fileName)) issueCount++;
            }

        }


        Assert.assertEquals(issueCount, 1);

    }


}

