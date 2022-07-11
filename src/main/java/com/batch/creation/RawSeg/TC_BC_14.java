package com.batch.creation.RawSeg;

import com.batch.creation.BatchCountValidator;
import com.batch.creation.DBEntryVerification;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;

public class TC_BC_14 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test()
    @Parameters({"batchConfigPath"})
    void validate(String batchConfigPath) throws IOException {
        //doubt
        //we need to go through all the generated manifest files and check whether any object is repeating or not

        JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());


        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";

        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        System.out.println("Latest Batch Creation Time: " + LatestBatchCreationTime);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        List<String> list = new ArrayList<String>();

        for (String str : GeneratedBatches) {
            JsonObject jsonObject = ManifestFileParser.getManifestDetails(s3Bucket, str);
            JsonArray batchObjects = jsonObject.get("batchObjects").getAsJsonArray();
            for (JsonElement arrayValues : batchObjects) {
                String value = arrayValues.getAsString();
                //System.out.println(value);
                if (!list.contains(value)) {
                    list.add(value);
                    //System.out.println(value);
                } else {
                    issueCount++;
                    System.out.println(value);
                }

                //System.out.println(str.getAsString().contains("RAW_D_15_S_202201310641_1202"));
            }
        }
        Assert.assertEquals(issueCount, 0);
    }
}