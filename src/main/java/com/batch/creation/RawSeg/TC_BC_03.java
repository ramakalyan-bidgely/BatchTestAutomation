package com.batch.creation.RawSeg;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.S3FileTransferHandler;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;

@Test()
public class TC_BC_03 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());

    @Test()
    @Parameters("batchConfigPath")
    public void validate(String batchConfigPath) throws IOException, InterruptedException {


        JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());


        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();

        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId + "/batch_id";

        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        Reporter.log("Latest Batch Creation Time: " + LatestBatchCreationTime,true);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        for (String str : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, str);
            Reporter.log("Validating batch entry in the table -> " + str,true);
            if (!DBEntryVerification.validate(UUID.fromString(jsonObject.get("batchId").getAsString()),jsonObject.get("batchCreationType").getAsString())) issueCount++;
        }
        Assert.assertEquals(issueCount, 0);
    }
}
