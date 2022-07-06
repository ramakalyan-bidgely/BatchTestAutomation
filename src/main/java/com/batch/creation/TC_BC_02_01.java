package com.batch.creation;/*
package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3Client;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.ManifestResponse;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.json.simple.JSONArray;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


*/
/**
 * @author Rama Kalyan
 *//*

@Test()

public class TC_BC_02_01 {
  */
/*
        Test_CaseID         : TC_BC_02
        Priority            : P0
        Area                : Batch Creation
        TestCaseName        : NoOfBatchesCreatedVerification
        Test Case Summary   : Verify expected number of SIZE_BASED manifest files generated for each pilot & component from the batch Creation service
        Steps               : "1. Create Batch configuration file with valid BatchCreationConfig for each pilot & component in the batchConfig element
                                  i.   Provide valid bucket path of configured pilot
                                  ii.  Valid prefix, directory_structure must be provided
                                  iii. Configure dataSizeInBytes value as 1,048,5760 (10Mb)
                                  iv. Configure intervalInSec value as 3600sec
                               2. Provide Batch Configuration file as Input for Batch Creation service
                               3. Upload pilot specific data files of 100, each of size 1 Mb in their corresponding s3 location"
        Expected Result    :  Batch com.batch.creation service should create batch manifest file (SIZE_BASED) for every 10Mb data has accumulated in the configured bucket. As files uploaded are of size 100Mb.
                               So 10 Batch manifest files (10 batch objects in each) should be generated in their corresponding location
                                    (ex: s3://{bucket_prefered_for_manifests}/batch_manifests/pilot_id=10009/component=ingestion/batchId={uuid})"
    *//*


    //val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

    //actual bucket should be from batchCreationConfig

    private Integer issueCount = 0;

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    public void validate(JsonObject batchConfig) throws InterruptedException {
        System.out.println("Helloworld");
        InputConfigParser ConfigParser = new InputConfigParser();
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String prefix = bc.getPrefix();
        Long dataSizeInBytes = bc.getDataSizeInBytes();
        AmazonS3Client amazons3Client = new AmazonS3Client();

        //AmazonS3URI SRC_URI = new AmazonS3URI(SRC_S3_PATH);

        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId;

        JSONArray ObjectsList = new JSONArray();

        Long DataAccumulatedSize = BatchCountValidator.getAccumulatedSize(pilotId, s3Bucket, prefix);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestList(pilotId, component, s3Bucket, manifest_prefix);

        Map<String, Integer> BMF_BCT_CNTS = new HashMap<String, Integer>();

        int SIZE_BASED_CNT = 0;
        int TIME_BASED_CNT = 0;

        long ExpectedNoOfBatches = DataAccumulatedSize / dataSizeInBytes;

        for (String manifestObject : GeneratedBatches) {
            JsonObject mObj = ManifestFileParser.getManifestFile(manifestObject);
            ManifestResponse mfResponse = ManifestFileParser.getManifestResponse(mObj);
            if (Objects.equals(ManifestResponse.getbatchCreationType(), "SIZE_BASED")) SIZE_BASED_CNT++;
            else if (Objects.equals(ManifestResponse.getbatchCreationType(), "TIME_BASED")) TIME_BASED_CNT++;
        }

        if (GeneratedBatches.size() >= ExpectedNoOfBatches) {
            System.out.println("Generated Batches : " + GeneratedBatches.size());
        } else {
            issueCount++;
        }
        String bctype = (SIZE_BASED_CNT > 0) ? "SIZE_BASED" : "TIME_BASED";
        Reporter.log("Data Accumulated Size -> " + DataAccumulatedSize + " dataSizeConfigured -> " + dataSizeInBytes + " Expected no.of batches -> " + ExpectedNoOfBatches + "Batch Creation Type -> " + bctype + " Batches Created ->  " + GeneratedBatches.size(), true);

        Assert.assertEquals(issueCount, 0);

    }


   */
/* def validate(ConfigJsonFilePath: String, batchInConfig: BatchInConfig, args: Array[String], InputDataFiles: String): mutable.Map[ArrayBuffer[String], Int] = {

        val InputBatchConfig: AmazonS3URI = new AmazonS3URI(ConfigJsonFilePath)
        var SRC_PATH: String = args.apply(2).trim
        val DEST_PATH: String = args.apply(3).trim


        val InpConfig = amazonS3Client.listObjects(InputBatchConfig.getBucket, InputBatchConfig.getKey).getObjectSummaries
        val batchConfigTime = InpConfig.last.getLastModified

        log("Validating number of batches com.batch.creation : ")
        val pilotId = batchInConfig.pilotId
        val component = batchInConfig.component
        val prefix = batchInConfig.batchCreationConfig.prefix
        val dataSizeInBytes = batchInConfig.batchCreationConfig.dataSizeInBytes


        val manifest_prefix = s"batch-manifests/pilot_id=$pilotId/component=$component/batchId"
        val s3Bucket = batchInConfig.batchCreationConfig.bucket
        val manifestObjs = new ArrayBuffer[String]()


        val result = scala.collection.mutable.Map[ArrayBuffer[String], Int]()

        val DEST_URI: AmazonS3URI = new AmazonS3URI(DEST_PATH)

        //S3 file handler
    *//*
*/
/* if (args.apply(0).trim == "s3") {
       val SRC_URI: AmazonS3URI = new AmazonS3URI(SRC_PATH)
       UploadedSize = S3FileTransferHandler.ProcessObjects(SRC_URI, DEST_URI, InputDataFiles, 0, intervalInSec)
     } else if (args.apply(0).trim == "l") {
       val SRC_DIR: String = SRC_PATH
       UploadedSize = S3FileUploadHandler.ProcessObjects(SRC_DIR, DEST_URI, InputDataFiles, 0)
     }*//*
*/
/*


        val DataAccumulatedSize: Long = BatchCountValidator.getAccumulatedSize(pilotId, component, s3Bucket, prefix, batchConfigTime)
        val GeneratedBatches: ArrayBuffer[String] = BatchCountValidator.getBatchManifestList(pilotId, component, s3Bucket, manifest_prefix, batchConfigTime)


        val ExpectedNoOfBatches: Long = DataAccumulatedSize / dataSizeInBytes
        if (GeneratedBatches.size >= ExpectedNoOfBatches) {
            log(s"Data Accumulated Size -> $DataAccumulatedSize, dataSizeConfigured -> $dataSizeInBytes , Expected no. of batches -> $ExpectedNoOfBatches, Batches Created -> {} - Test Case Passed !", GeneratedBatches.size)
        } else {
            log(s"Data Accumulated Size -> $DataAccumulatedSize, dataSizeConfigured -> $dataSizeInBytes , Expected no. of batches -> $ExpectedNoOfBatches, Batches Created -> {} - Test Case Failed !", GeneratedBatches.size)
        }
        result ++= Map(manifestObjs -> GeneratedBatches.size)
        result
    }*//*

}
*/
