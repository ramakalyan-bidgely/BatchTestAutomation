package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.utils.*;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * @author Rama Kalyan
 */
@Test()
public class TC_BC_03 extends MainDataProvider {


    /*

     Test_CaseID		    :	TC_BC_03
     Priority	      	  :	P0
     Area				        :	Batch Creation
     TestCaseName	  	  :	NoOfBatchesCreated_WithDiff_BC_Type_Verification
     TestCaseSummary	  :	Verify expected number of TIME_BASED/SIZE_BASED (Combination) manifest files generated for each pilot & component from the batch Creation service
     Steps	        		:	1. Create Batch configuration file with valid BatchCreationConfig for each pilot & component in the batchConfig element
                             i.   Provide valid bucket path of configured pilot
                             ii.  Valid prefix, directory_structure must be provided
                             iii. Configure dataSizeInBytes value as 209713400 (200 Mb)
                             iv. Configure intervalInSec value as 300 sec
                         2. Provide Batch Configuration file as Input for Batch Creation service
                         3. Upload pilot specific data files of 1100, each of size 1 Mb in their corresponding s3 location
     ExpectedResult	    :	Batch com.batch.creation service should create batch manifest file (TIME_BASED) for every 300sec (5min)  interval with the data accumulated in the configured bucket. two scenarios can be considered here
                         1. due to network latency assuming 2 sec for each data file upload
                               -> 6 batch manifest files(with approximately 150 batch objects in it) (TIME_BASED)
                         2. due to no network latency assuming 1 sec for each data file upload (200 objects accumulated before time interval then com.batch.creation service will consider size_based manifest can be generated or not. if so it will generate SIZE_BASED manifest file)
                              -> 1 batch manifest file with leftover 200 objects in it) (SIZE_BASED) will be created
                         (ex: s3://{bucket_preferred_for_manifests}/batch_manifests/pilot_id=10035/component=ingestion/batchId={uuid})
 */


    private Integer issueCount = 0;

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    public void validate(JsonObject batchConfig) throws InterruptedException {

        InputConfigParser ConfigParser = new InputConfigParser();
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String bcConfigPrefix = bc.getPrefix();
        Long dataSizeInBytes = bc.getDataSizeInBytes();
        AmazonS3Client amazons3Client = new AmazonS3Client();
        String s3Prefix = "s3://";
        String manifest_prefix = "s3://bidgely-adhoc-batch-qa/batch-manifests/pilot_id=" + pilotId;
        //Long DataAccumulatedSize = BatchCountValidator.getAccumulatedSize(pilotId, s3Bucket, prefix);
        AmazonS3URI DEST_URI = new AmazonS3URI(s3Prefix + s3Bucket + "/" + bcConfigPrefix + new SimpleDateFormat("yyyy/MM/dd").format(new Date()) + "/");
        String Dir = "D:\\ProS\\Bidgely\\DataIngestion\\TestCasesforBatch\\Scenario1";
        long DataAccumulatedSize = S3FileTransferHandler.TransferFiles(DEST_URI, Dir);
        System.out.println("Data Accumulated Size : " + DataAccumulatedSize);

        //Long DataAccumulatedSize = BatchCountValidator.getAccumulatedSize(pilotId, s3Bucket, prefix);

        //Invocation of Batch Craetion service will be triggered here


        //Wait Batch creation service to be completed
        Thread.sleep(10000);
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
        Map<String, Integer> BMF_BCT_CNTS = new HashMap<String, Integer>();

        int SIZE_BASED_CNT = 0;
        int TIME_BASED_CNT = 0;


        Iterator mObjs = GeneratedBatches.iterator();
        while (mObjs.hasNext()) {
            System.out.println(mObjs.next());
        }
        long ExpectedNoOfBatches = DataAccumulatedSize / dataSizeInBytes;

        for (String manifestObject : GeneratedBatches) {
            JsonObject mObj = ManifestFileParser.getManifestFile(manifestObject);
            ManifestResponse mfResponse = ManifestFileParser.getManifestResponse(mObj);
            if (Objects.equals(ManifestResponse.getbatchCreationType(), "SIZE_BASED")) SIZE_BASED_CNT++;
            else if (Objects.equals(ManifestResponse.getbatchCreationType(), "TIME_BASED")) TIME_BASED_CNT++;
        }

        String bctype = (SIZE_BASED_CNT > 0) ? "SIZE_BASED" : "TIME_BASED";
        if (GeneratedBatches.size() >= ExpectedNoOfBatches) {
            System.out.println("Generated Batches : " + GeneratedBatches.size());
        } else {
            issueCount++;
        }
        Reporter.log("Data Accumulated Size -> " + DataAccumulatedSize + " dataSizeConfigured -> " + dataSizeInBytes + " Expected no.of batches -> " + ExpectedNoOfBatches + " Batches Created ->  " + GeneratedBatches.size(), true);
        Assert.assertEquals(issueCount, 0);

    }


/*val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  implicit val formats: DefaultFormats.type = DefaultFormats

  def validate(ConfigJsonFilePath : String, batchInConfig: BatchInConfig, args: Array[String], InputDataFiles: String): Int = {

    val InputBatchConfig: AmazonS3URI = new AmazonS3URI(ConfigJsonFilePath)
    var SRC_PATH: String = args.apply(2).trim
    val DEST_PATH: String = args.apply(3).trim


    val InpConfig = amazonS3Client.listObjects(InputBatchConfig.getBucket, InputBatchConfig.getKey).getObjectSummaries
    val batchConfigTime = InpConfig.last.getLastModified

    Reporter.log("Validating number of batches com.batch.creation : ")
    val pilotId = batchInConfig.pilotId
    val component = batchInConfig.component
    val prefix = batchInConfig.batchCreationConfig.prefix
    val dataSizeInBytes = batchInConfig.batchCreationConfig.dataSizeInBytes


    val manifest_prefix = s"batch-manifests/pilot_id=$pilotId/component=$component/batchId"
    val s3Bucket = batchInConfig.batchCreationConfig.bucket
    val manifestObjs = new ArrayBuffer[String]()



    val DEST_URI: AmazonS3URI = new AmazonS3URI(DEST_PATH)
    //S3 file handler

    if (args.apply(0).trim == "s3") {
       val SRC_URI: AmazonS3URI = new AmazonS3URI(SRC_PATH)
       UploadedSize = S3FileTransferHandler.ProcessObjects(SRC_URI, DEST_URI, InputDataFiles, 0, intervalInSec)
     } else if (args.apply(0).trim == "l") {
       val SRC_DIR: String = SRC_PATH
       UploadedSize = S3FileUploadHandler.ProcessObjects(SRC_DIR, DEST_URI, InputDataFiles, 0)
     }

/*


    val DataAccumulatedSize:Long =BatchCountValidator.getAccumulatedSize(pilotId,component,s3Bucket,prefix,batchConfigTime)

    val GeneratedBatches:ArrayBuffer[String]=BatchCountValidator.getBatchManifestList(pilotId,component,s3Bucket,manifest_prefix,batchConfigTime)


            if(GeneratedBatches.nonEmpty)

    {
        GeneratedBatches.foreach(manifestObj = > {
                val json_data = BatchManifestFileHandler.getManifestData(s3Bucket, manifestObj)
                val jsonobj = json.parse(json_data).asInstanceOf[JObject].values
                val batchSizeInBytes = jsonobj(BATCH_SIZE_IN_BYTES)
                val batchCreationType = jsonobj(BATCH_CREATION_TYPE)
                SIZE_BASED_CNT += ( if (batchCreationType.equals("SIZE_BASED")) 1
    else 0)
        TIME_BASED_CNT += ( if (batchCreationType.equals("TIME_BASED")) 1
    else 0)

      })
        BMF_BCT_CNTS++ = Map("SIZE_BASED" ->SIZE_BASED_CNT, "TIME_BASED" ->TIME_BASED_CNT)
        val ExpectedNoOfBatches:Long = DataAccumulatedSize / dataSizeInBytes
        if (GeneratedBatches.size >= (DataAccumulatedSize / dataSizeInBytes)) {
            Reporter.log(s"Data Accumulated Size -> $DataAccumulatedSize, dataSizeConfigured -> $dataSizeInBytes, Expected no. of batches -> $ExpectedNoOfBatches, SIZE_BASED -> ${BMF_BCT_CNTS("SIZE_BASED")}, TIME_BASED -> ${BMF_BCT_CNTS("TIME_BASED")} , Batches Created -> {} - Test Case Passed !", GeneratedBatches.size)
        } else {
            Reporter.log(s"Data Accumulated Size -> $DataAccumulatedSize, dataSizeConfigured -> $dataSizeInBytes, Expected no. of batches -> $ExpectedNoOfBatches, SIZE_BASED -> ${BMF_BCT_CNTS("SIZE_BASED")}, TIME_BASED -> ${BMF_BCT_CNTS("TIME_BASED")} , Batches Created -> {} - Test Case Failed !", GeneratedBatches.size)
        }
    }
    if(GeneratedBatches.isEmpty)

    {
        Reporter.log("No manifest files found - Test case Failed !")
    }

    GeneratedBatches.size
}
*/


}
