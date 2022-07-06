package com.batch.creation;


/**
 * @author Rama Kalyan
 */
public class TC_BC_09 {

  /*
      Test_CaseID		  :	TC_BC_09
      Priority		    :	P1
      Area				    :	Batch Creation
      TestCaseName		:	LatestObjectDetail_Verification
      TestCaseSummary	:	Verify latest object details from the manifest file are valid or not
      Steps			      :	1. Read Latest object details from manifest file
                        2. Validate the same latest object_key with latest modified time are matching with batchGlobalStatus table
                        3. Check latest object's availability in the s3 location
      ExpectedResult	:	Latest object should be available in the s3 bucket

   */


/*
  def validate(ConfigJsonFilePath : String, batchInConfig: BatchInConfig, args: Array[String], InputDataFiles: String): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)
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
        val jsonobj = json.parse(json_data).asInstanceOf[JObject].values
        val latestObjectKey = jsonobj(LATEST_OBJECT_KEY).toString
        val latestObjectModifiedTime = jsonobj(LATEST_OBJECT_MODIFIED_TIME)
        val batchId = jsonobj(BATCH_ID)

        val latestObjDetails: mutable.Map[String, Any] = BatchInfoDB.getLatestObjectDetails(BatchSnapshot, pilotId, component)

        if (latestObjDetails.nonEmpty) {
          if (latestObjectKey.equals(latestObjDetails(latestObjectKey))) {
            val isLatestObjectKeyExists = amazonS3Client.doesObjectExist(s3Bucket, manifestObj)
            Reporter.log("Latest Object key {} available in the bucket -> {} - Test case Passed !", latestObjectKey, isLatestObjectKeyExists)
          } else {
            Reporter.log("Latest Object Key is different from the actual latest object key in manifest file - Test case failed !")
          }
        } else if (latestObjDetails.isEmpty) {
          Reporter.log("Batch Entry for batch id - {} is missing in Database - Test case failed !", batchId)
        }
      })
    }
  }
*/

}
