package com.batch.creation;/*
package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.batch.utils.ManifestResponse;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.json.simple.JSONArray;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.*;

import static com.batch.utils.sql.batch.BatchDetails.getLatest_modified_time;

*/
/**
 * @author Rama Kalyan
 *//*

public class TC_BC_08 {

    private Integer issueCount = 0;

    @Parameters({"SRC_S3_PATH", "DEST_S3_PATH"})
    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    public void validate(JsonObject batchConfig, String SRC_S3_PATH) throws InterruptedException {

        InputConfigParser ConfigParser = new InputConfigParser();
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String prefix = bc.getPrefix();
        Long dataSizeInBytes = bc.getDataSizeInBytes();
        AmazonS3Client amazons3Client = new AmazonS3Client();

        AmazonS3URI SRC_URI = new AmazonS3URI(SRC_S3_PATH);

        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId;

        JSONArray ObjectsList = new JSONArray();

        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(prefix).withDelimiter("/");
        ListObjectsV2Result fileObjs = amazons3Client.listObjectsV2(req);
        List<S3ObjectSummary> summaries = fileObjs.getObjectSummaries();
        List<String> manifestObjs = new ArrayList<String>();

        for (S3ObjectSummary summary : summaries) {
            if (getLatest_modified_time().compareTo(summary.getLastModified()) == 0) {
                manifestObjs.add(summary.getKey());
            }
        }

        //amazons3Client.copyObject(SRC_URI.getBucket(), SRC_URI.getKey() + "/" + objName.toString, DEST_URI.getBucket, DEST_URI.getKey + "/" + objName.toString);
        Thread.sleep(10000);

       */
/* if (args.apply(0).trim == "s3") {
            val SRC_URI: AmazonS3URI = new AmazonS3URI(SRC_PATH);
            UploadedSize = S3FileTransferHandler.ProcessObjects(SRC_URI, DEST_URI, InputDataFiles, 0, intervalInSec);
        } else if (args.apply(0).trim == "l") {
            val SRC_DIR: String = SRC_PATH
            UploadedSize = S3FileUploadHandler.ProcessObjects(SRC_DIR, DEST_URI, InputDataFiles, 0)
        }*//*



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
            Reporter.log("Generated Batches : " + GeneratedBatches.size(),true);
        } else {
            issueCount++;
        }
        String bctype = (SIZE_BASED_CNT > 0) ? "SIZE_BASED" : "TIME_BASED";
        Reporter.log("Data Accumulated Size -> " + DataAccumulatedSize + " dataSizeConfigured -> " + dataSizeInBytes + " Expected no.of batches -> " + ExpectedNoOfBatches + "Batch Creation Type -> " + bctype + " Batches Created ->  " + GeneratedBatches.size(), true);

        Assert.assertEquals(issueCount, 0);


    }

  */
/*
      Test_CaseID		  :	TC_BC_08
      Priority		    :	P0
      Area				    :	Batch Creation
      TestCaseName		:	DuplicateObjects_Verification
      TestCaseSummary	:	Verifying if any duplicate objects present across all manifest files of that particular {pilot}-{component}
      Steps			      :	1. Read or get values of batchObjects key from all successfully generated manifest files.
                        2. Check for any duplicates entries in the objects
      ExpectedResult	:	Duplicate entries of objects should not be there across manifest files

   *//*

*/
/*
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  def validate(ConfigJsonFilePath : String, batchInConfig: BatchInConfig, args: Array[String], InputDataFiles: String): Unit = {

    Reporter.log("Validating number of batches com.batch.creation : ")
    val pilotId = batchInConfig.pilotId
    val component = batchInConfig.component
    val prefix = batchInConfig.batchCreationConfig.prefix
    val dataSizeInBytes = batchInConfig.batchCreationConfig.dataSizeInBytes


    val manifest_prefix = s"batch-manifests/pilot_id=$pilotId/component=$component/batchId"
    val s3Bucket = batchInConfig.batchCreationConfig.bucket
    val manifestObjs = new ArrayBuffer[String]()



    val req: ListObjectsV2Request = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(prefix).withDelimiter("/")
    val fileobjs = amazonS3Client.listObjectsV2(req)
    val summaries = fileobjs.getObjectSummaries

    summaries.forEach(o => manifestObjs += o.getKey)

    Reporter.log("Verifying for any duplicate Objects across all batch Manifest files")
    val DUP_OBJS = BatchManifestFileHandler.getDuplicateObjects(s3Bucket, manifestObjs)

    if (DUP_OBJS.length > 0) {
      Reporter.log("Duplicate Objects found: \n" + DUP_OBJS.mkString("\n"))
      Reporter.log("Test Case Failed !")
    } else {
      Reporter.log("No Duplicates found. Test Case passed !")
    }

  }
*//*

}
*/
