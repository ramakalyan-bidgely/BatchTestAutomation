package com.batch.creation.RawSeg;

import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

@Test
public class TC_BC_16 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private final Integer issueCount = 0;

    @Test()
    public void validate() throws IOException {
        int issueCount=0;
        InputConfigParser ConfigParser = new InputConfigParser();
        String jsonFilePath = "D:\\TEST DATA\\TC_BC_16\\input_config.json";
        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);
        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();
        // JsonObject value =InputConfigParser.getBatchInputs(batchConfig.get("batchConfigs").getAsString());
        //System.out.println(value);

        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String manifest_prefix = bc.getPrefix();
        //String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId + "/batchId";
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);

        System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        for(String str: GeneratedBatches){
            JsonObject jsonObject= ManifestFileParser.batchConfigDetails(s3Bucket,str);
            if(!DBEntryVerification.validate(UUID.fromString(jsonObject.get("batchId").getAsString())))issueCount++;
        }

        Assert.assertEquals(issueCount,0);
    }
}
