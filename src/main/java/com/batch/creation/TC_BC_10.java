package com.batch.creation;


import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.annotations.Test;

/**
 * @author Rama Kalyan
 */
public class TC_BC_10 extends MainDataProvider {
  /*
    Test_CaseID		  :	TC_BC_10
    Priority		    :	P1
    Area				    :	Batch Creation
    TestCaseName		:	EMRParameters_Verification
    TestCaseSummary	:	Verify appropriate cluster name, other parameters are generated in the manifest file
    Steps			      :	1. Read successfully generated manifest file
                      2. Check the clusterName element in the emr_params is valid or not
    ExpectedResult  :	ClusterName of EMR Param must be valid with pilot_id - component - batchId
  */

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    public void validate(JsonObject batchConfig) {


    }


    /*
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)
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

    if (manifestObjs.nonEmpty) {
      manifestObjs.foreach(manifestObj => {
        val json_data = BatchManifestFileHandler.getManifestData(s3Bucket, manifestObj)
        val jsonobj = json.parse(json_data)
        val batchId = (jsonobj \\ BATCH_ID).values(BATCH_ID)
        val clusterName = (jsonobj \\ CLUSTER_NAME).values(CLUSTER_NAME)
        if (clusterName == (pilotId + "-" + component + "-" + batchId)) {
          Reporter.log("Cluster Name {} is valid - Test case passed !", clusterName)
        } else {
          Reporter.log("Cluster Name {} is invalid - Test case failed", clusterName)
        }
      })
    }
    if (manifestObjs.isEmpty) {
      Reporter.log("No manifest files found - Test case Failed !")
    }
    manifestObjs.size
  }*/
}
