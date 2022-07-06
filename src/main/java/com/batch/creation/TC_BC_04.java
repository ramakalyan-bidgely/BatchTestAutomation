package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3Client;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.ManifestResponse;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.batch.utils.VariableCollections.manifestObjs;

/**
 * @author Rama Kalyan
 */
public class TC_BC_04 {

  /*
      Test_CaseID		  :	TC_BC_04
      Priority		    :	P0
      Area				    :	Batch Creation
      TestCaseName		:	DuplicateCreationOfBatches_Verification
      TestCaseSummary	:	Verify whether any two batch manifest generated for same objects if files accumulation in the directory happens at the same interval configured
      Steps			      :	1. Create User configuration file with valid BatchCreationConfig for each pilot & component in the batchConfig element
                            i.   Provide valid bucket path of configured pilot
                            ii.  Valid prefix, directory_structure must be provided
                            iii. Configure dataSizeInBytes value as 314572800 (300 Mb)
                            iv.  Configure intervalInSec value as 600 sec
                        2. Provide Batch Configuration file as Input for Batch Creation service
                        3. Upload pilot specific data files of 300, each of size 1 Mb in their corresponding s3 location
      ExpectedResult	:	Batch com.batch.creation service should create batch manifest file (TIME_BASED) for every 600sec (10min)  interval with the data accumulated in the configured bucket.
                          i. Assuming 2 sec for each data file upload, all 300 mb gets accumulated in exact threshold of 600sec. Service should create either SIZE_BASED manifest file by considering size configured or TIME_BASED manifest file by considering threshold of 600sec. But It should not create both the files with same objects and with different batchCreationTypes
                        (ex: s3://{bucket_prefered_for_manifests}/batch_manifests/pilot_id=10035/component=ingestion/batchId={uuid})

   */


    private Integer issueCount = 0;

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    public void validate(JsonObject batchConfig) {

        InputConfigParser ConfigParser = new InputConfigParser();
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String prefix = bc.getPrefix();
        Long dataSizeInBytes = bc.getDataSizeInBytes();
        AmazonS3Client amazons3Client = new AmazonS3Client();

        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId;

        Long DataAccumulatedSize = BatchCountValidator.getAccumulatedSize(pilotId, s3Bucket, manifest_prefix);


        Map<String, Integer> BMF_BCT_CNTS = new HashMap<String, Integer>();

        int SIZE_BASED_CNT = 0;
        int TIME_BASED_CNT = 0;

        int OBJ_CNT = 0;

        long ExpectedNoOfBatches = DataAccumulatedSize / dataSizeInBytes;

        for (String manifestObject : manifestObjs) {
            JsonObject mObj = ManifestFileParser.getManifestFile(manifestObject);
            ManifestResponse mfResponse = ManifestFileParser.getManifestResponse(mObj);

            if (Objects.equals(ManifestResponse.getbatchCreationType(), "SIZE_BASED")) SIZE_BASED_CNT++;
            else if (Objects.equals(ManifestResponse.getbatchCreationType(), "TIME_BASED")) TIME_BASED_CNT++;

        }

        if (manifestObjs.size() < ExpectedNoOfBatches) {
            issueCount++;
        }
        Reporter.log("Data Accumulated Size -> " + DataAccumulatedSize + " dataSizeConfigured -> " + dataSizeInBytes + " Expected no.of batches -> " + ExpectedNoOfBatches + " Batches Created ->  " + manifestObjs.size(), true);
        Assert.assertEquals(issueCount, 0);
    }

}
