package com.batch.utils

/**
 * @author Rama Kalyan
 */

import com.amazonaws.services.s3.model.S3Object
import net.liftweb.json._
import org.json.{JSONArray, JSONObject}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer


object BatchManifestFileHandler {

  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  implicit val formats: DefaultFormats.type = DefaultFormats

  def getManifestVals(s3BucketPath: String, JsonFileName: String, keyval: String, Subkey: String): Any = {
    val result = getJsonInfo(s3BucketPath, JsonFileName, keyval, Subkey)
    result
  }

  // Pulling Json information from the s3 Object (json file)
  def getJsonInfo(s3BucketPath: String, JsonFileName: String, keyval: String, Subkey: String): Any = {
    val s3Object: S3Object = amazonS3Client.getObject(s3BucketPath, JsonFileName)
    val json_data = IOUtils.toString(s3Object.getObjectContent)
    val jsonobj = parse(json_data) \\ keyval

    val obj = if (Subkey == null) keyval else Subkey

    val result = obj match {
      case "workerInstanceCount" => jsonobj.children.map(_.extract[EMRParamsConfig].workerInstanceCount)
      case "diskSizeInGb" => jsonobj.children.map(_.extract[EMRParamsConfig].diskSizeInGb)
      case "volumePerInstance" => jsonobj.children.map(_.extract[EMRParamsConfig].volumePerInstance)
      case "fleetType" => jsonobj.children.map(_.extract[EMRParamsConfig].fleetType)
      case "skipSucceededTasksOnRetry" => jsonobj.children.map(_.extract[ScheduleConfig].skipSucceededTasksOnRetry)
      case "IsNextBatchDependentOnPrev" => jsonobj.children.map(_.extract[ScheduleConfig].isNextBatchDependentOnPrev)
      case "parallelBatchesIfIndependent" => jsonobj.children.map(_.extract[ScheduleConfig].parallelBatchesIfIndependent)
      case "maxTries" => jsonobj.children.map(_.extract[ScheduleConfig].maxTries)
      case "dagId" => jsonobj.children.map(_.extract[ScheduleConfig].dagId)
      case _ => null
      //below keyval is for parent nodes in the json
      case keyval => jsonobj.values(keyval)
    }
    result
  }

  def getManifestData(s3BucketPath: String, JsonFileName: String): String = {
    val s3Object: S3Object = amazonS3Client.getObject(s3BucketPath, JsonFileName)
    val json_data = IOUtils.toString(s3Object.getObjectContent)
    json_data
  }

  //wrapper function to identify duplicate Objects across all batch manifest files
  def getDuplicateObjects(s3Bucket: String, manifestObjs: ArrayBuffer[String]): ArrayBuffer[String] = {
    val batchObjs = ArrayBuffer[String]()
    manifestObjs.foreach {
      manifestObj =>
        getBatchObjects(s3Bucket, manifestObj).forEach(o => batchObjs += o.toString)
    }
    batchObjs diff batchObjs.distinct
  }

  def getBatchObjects(s3BucketPath: String, JsonFileName: String): JSONArray = {
    val s3Object: S3Object = amazonS3Client.getObject(s3BucketPath, JsonFileName)
    val json_data = IOUtils.toString(s3Object.getObjectContent)
    val jsonobj = new JSONObject(json_data)
    val ObjectsList = jsonobj.getJSONArray(BATCH_OBJECTS)
    ObjectsList
  }
}
