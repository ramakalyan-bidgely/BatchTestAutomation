package com.batch.creation;


import com.amazonaws.services.s3.AmazonS3Client;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.annotations.Test;

/**
 * @author Rama Kalyan
 */
public class TC_BC_05 {


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



    }
  /*
    Test_CaseID		  :	TC_BC_05
    Priority		    :	P0
    Area				    :	Batch Creation
    TestCaseName		:	BatchCreation_WithLargeFile_Verification
    TestCaseSummary	:	Verify generation of manifest files when data of single large file doesn't suitable for both the intervalInSec configuration & dataSizeInBytes in the Config file
    Steps			      :	1. Create Batch configuration file with valid BatchCreationConfig for each pilot & component in the batchConfig element
                          i.   Provide valid bucket path of configured pilot
                          ii.  Valid prefix, directory_structure must be provided
                          iii. Configure dataSizeInBytes value as 104857600 (100 Mb)
                          iv. Configure intervalInSec value as 10 sec
                      2. Provide Batch Configuration file as Input for Batch Creation service
                      3. Upload one pilot specific data file, of size 128 Mb in their corresponding s3 location
    ExpectedResult	:	Batch Creation service may fail to generate manifest file under below conditions,
                        1. Data Object/file which accumulated in the data bucket with file size of exceeded configured dataSizeInBytes threshold &
                        2. If data hasn't been accumulated by the configured time threshold

                      -> This data file  will be considered as one of the batch object in next manifest file or manifest file(s) generated in the next interval if the file has been accumulated by the next interval limits

   */

  /*val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)
  def validate(ConfigJsonFilePath: String, batchInConfig: BatchInConfig, args: Array[String], InputDataFiles: String): Int = {

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
    *//*  if (args.apply(0).trim == "s3") {
        val SRC_URI: AmazonS3URI = new AmazonS3URI(SRC_PATH)
        UploadedSize = S3FileTransferHandler.ProcessObjects(SRC_URI, DEST_URI, InputDataFiles, 0, intervalInSec)
      } else if (args.apply(0).trim == "l") {
        val SRC_DIR: String = SRC_PATH
        UploadedSize = S3FileUploadHandler.ProcessObjects(SRC_DIR, DEST_URI, InputDataFiles, 0)
      }*//*


    val DataAccumulatedSize: Long = BatchCountValidator.getAccumulatedSize(pilotId, component, s3Bucket, prefix, batchConfigTime)

    val GeneratedBatches: ArrayBuffer[String] = BatchCountValidator.getBatchManifestList(pilotId, component, s3Bucket, manifest_prefix, batchConfigTime)
    var BMF_BCT_CNTS = Map[String, Int]()

    var SIZE_BASED_CNT = 0
    var TIME_BASED_CNT = 0

    if (GeneratedBatches.nonEmpty) {
      GeneratedBatches.foreach(manifestObj => {
        val json_data = BatchManifestFileHandler.getManifestData(s3Bucket, manifestObj)
        val jsonobj = json.parse(json_data).asInstanceOf[JObject].values
        val batchCreationType = jsonobj(BATCH_CREATION_TYPE)
        SIZE_BASED_CNT += (if (batchCreationType.equals("SIZE_BASED")) 1 else 0)
        TIME_BASED_CNT += (if (batchCreationType.equals("TIME_BASED")) 1 else 0)
      })
      BMF_BCT_CNTS ++= Map("SIZE_BASED" -> SIZE_BASED_CNT, "TIME_BASED" -> TIME_BASED_CNT)
      val ExpectedNoOfBatches: Long = (DataAccumulatedSize / dataSizeInBytes)
      if (GeneratedBatches.size >= ExpectedNoOfBatches) {
        Reporter.log(s"DataAccumulated Size -> $DataAccumulatedSize, dataSizeConfigured -> $dataSizeInBytes, Expected no. of batches -> $ExpectedNoOfBatches, SIZE_BASED -> ${BMF_BCT_CNTS("SIZE_BASED")}, TIME_BASED -> ${BMF_BCT_CNTS("TIME_BASED")} , Batches Created -> {} - Test Case Passed !", GeneratedBatches.size)
      } else {
        Reporter.log(s"DataAccumulated Size -> $DataAccumulatedSize, dataSizeConfigured -> $dataSizeInBytes, Expected no. of batches -> $ExpectedNoOfBatches, SIZE_BASED -> ${BMF_BCT_CNTS("SIZE_BASED")}, TIME_BASED -> ${BMF_BCT_CNTS("TIME_BASED")} , Batches Created -> {} - Test Case Failed !", GeneratedBatches.size)
      }
    }
    if (GeneratedBatches.isEmpty) {
      Reporter.log("No manifest files found - Test case Failed !")
    }
    GeneratedBatches.size
  }*/
}
