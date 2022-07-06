package com.batch.creation;


import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.annotations.Test;

/**
 * @author Rama Kalyan
 */
public class TC_BC_11 {

    private final Integer issueCount = 0;

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    public void validate(JsonObject batchConfig) {

    }

  /*
      Test_CaseID		    :	TC_BC_11
      Priority		      :	P1
      Area				      :	Batch Creation
      TestCaseName		  :	ManifestSteps_Verification
      TestCaseSummary	  :	Verify valid stepConfigs, sparkConfigs are generated or not
      Steps			        :	1. Read successfully generated manifest file
                          2. Check the generated stepConfigs, sparkConfigs,arguments are valid or not
      ExpectedResult	  :	Steps, spark configs generated in the manifest file must be valid, else it would cause failure in batch run or instability

  * */

  /*val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)
  implicit val formats: DefaultFormats.type = DefaultFormats

  def validate(ConfigJsonFilePath: String, batchInConfig: BatchInConfig, args: Array[String], InputDataFiles: String): Int = {
    val InputBatchConfig: AmazonS3URI = new AmazonS3URI(ConfigJsonFilePath)
    var SRC_PATH: String = args.apply(2).trim
    val DEST_PATH: String = args.apply(3).trim


    val InpConfig = amazonS3Client.listObjects(InputBatchConfig.getBucket, InputBatchConfig.getKey).getObjectSummaries
    val batchConfigTime = InpConfig.last.getLastModified

    val pilotId = batchInConfig.pilotId
    val component = batchInConfig.component
    val prefix = batchInConfig.batchCreationConfig.prefix
    val dataSizeInBytes = batchInConfig.batchCreationConfig.dataSizeInBytes


    val manifest_prefix = s"batch-manifests/pilot_id=$pilotId/component=$component/batchId"
    val s3Bucket = batchInConfig.batchCreationConfig.bucket
    val manifestObjs = new ArrayBuffer[String]()



    val DEST_URI: AmazonS3URI = new AmazonS3URI(DEST_PATH)


    //S3 file handler
    *//*if (args.apply(0).trim == "s3") {
      val SRC_URI: AmazonS3URI = new AmazonS3URI(SRC_PATH)
      UploadedSize = S3FileTransferHandler.ProcessObjects(SRC_URI, DEST_URI, InputDataFiles, 0)
    } else if (args.apply(0).trim == "l") {
      val SRC_DIR: String = SRC_PATH
      UploadedSize = S3FileUploadHandler.ProcessObjects(SRC_DIR, DEST_URI, InputDataFiles, 0)
    }*//*
    val req: ListObjectsV2Request = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(prefix).withDelimiter("/")
    val fileobjs = amazonS3Client.listObjectsV2(req)
    val summaries = fileobjs.getObjectSummaries

    summaries.foreach { o =>
      if (o.getLastModified >= batchConfigTime) {
        manifestObjs += o.getKey
      }
    }


    if (manifestObjs.nonEmpty) manifestObjs.foreach(manifestObj => {
      val json_data = BatchManifestFileHandler.getManifestData(s3Bucket, manifestObj)
      val jsonobj = json.parse(json_data)

      val stepConfigs = (jsonobj \\ "stepConfigs").children


      for (elem <- stepConfigs) {
        val step_name = elem.extract[StepConfig].stepName
        val step_params: StepParamsConfig = elem.extract[StepConfig].stepParams
        val sparkConfigs = step_params.sparkConfigs
        val arguments = step_params.arguments

        Reporter.log("Validating Manifest file -> {} ", manifestObj)
        if (step_name.contains(component) == false) {
          Reporter.log("Step Name is Invalid - Test case failed !")
          break
        }else if(sparkConfigs.isEmpty){
          Reporter.log("Spark configs not generated - Test case failed !")
          break
        }else if(arguments.isEmpty){
          Reporter.log("Arguments not generated - Test case failed !")
          break
        }else{
          Reporter.log("All step configurations are provided - Test case Passed !")
        }
      }
    })
    if (manifestObjs.isEmpty) {
      Reporter.log("No manifest files found - Test case Failed !")
    }
    manifestObjs.size
  }
*/
}
