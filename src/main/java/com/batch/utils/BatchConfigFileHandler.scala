package com.batch.utils

/**
 * @author Rama Kalyan
 */

import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.S3Object
import net.liftweb.json
import net.liftweb.json.{DefaultFormats, Formats, JValue}
import org.slf4j.{Logger, LoggerFactory}

object BatchConfigFileHandler {
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  implicit val formats: Formats = DefaultFormats + new JKeySerializer

  def getCreationVals(s3BucketPath: String, JsonFileName: String, keyval: String, Subkey: String) = {
    getJsonInfo(s3BucketPath, JsonFileName, keyval, Subkey)
  }

  def main(args: Array[String]): Unit = {

    val bc_s3BucketPath: String = args.apply(0).trim
    val bc_JsonFileName: String = args.apply(1).trim


    // Validate  creation
    val res = getJsonInfo(s3BucketPath = bc_s3BucketPath, JsonFileName = bc_JsonFileName, keyval = "batchCreationConfig", Subkey = null)
    logger.info("Result : {}", res)


  }

  // Pulling Json information from the s3 Object (json file)
  def getJsonInfo(s3BucketPath: String, JsonFileName: String, keyval: String, Subkey: String): Any = {
    val s3Object: S3Object = amazonS3Client.getObject(s3BucketPath, JsonFileName)
    val json_data = IOUtils.toString(s3Object.getObjectContent)
    val jsonobj = (json.parse(json_data) \\ keyval).children

    var bucket: String = null
    var prefix: String = null
    var delimiter: String = null
    var directoryStructure: String = null
    var dataSizeInBytes: Long = 0
    var intervalInSec: Int = 0

    for (elem <- jsonobj) {
      val cc = elem.extract[CreationConfig]
      if (Subkey == null) {
        logger.info(
          s"""
             | bucket : ${cc.bucket}
             | prefix : ${cc.prefix}
             | directoryStructure : ${cc.directoryStructure}
             | data_size_in_mb : ${cc.dataSizeInBytes}
             | interval_in_sec : ${cc.intervalInSec}
             |""".stripMargin)
      }
      if (amazonS3Client.doesBucketExist(cc.bucket)) {
        logger.info("{} Bucket is available - Case Passed !", cc.bucket)
      } else {
        logger.info("{} Bucket does not exist - Case Failed !", cc.bucket)
      }
      bucket = cc.bucket
      prefix = cc.prefix
      directoryStructure = cc.directoryStructure
      dataSizeInBytes = cc.dataSizeInBytes
      intervalInSec = cc.intervalInSec
    }
    val result = Subkey match {
      case "bucket" => bucket
      case "prefix" => prefix
      case "delimiter" => delimiter
      case "directoryStructure" => directoryStructure
      case "dataSizeInBytes" => dataSizeInBytes
      case "intervalInSec" => intervalInSec
      case _ => null
    }
    result
  }

  def getJsonData(s3BucketPath: String, JsonFileName: String): String = {
    val s3Object: S3Object = amazonS3Client.getObject(s3BucketPath, JsonFileName)
    val json_data = IOUtils.toString(s3Object.getObjectContent)
    json_data
  }
/*

  def getBatchConfigs(JsonFilePath: String): Option[BatchInConfigs] = {
    implicit val formats: Formats = DefaultFormats + new JKeySerializer
    var json_data: String = null
    if (JsonFilePath.startsWith("s3://")) {
      val CONFIG_URI= new AmazonS3URI(JsonFilePath)
      val s3Object: S3Object = amazonS3Client.getObject(CONFIG_URI.getBucket, JsonFilePath)
      json_data = IOUtils.toString(s3Object.getObjectContent())
    } else {
      json_data = scala.io.Source.fromFile(JsonFilePath).mkString
    }
    val jsonobj = json.parse(json_data)
    Option(jsonobj.extract[BatchInConfigs])

  }*/


  def getBatchConfigs[T >: BatchInConfigs](JsonFilePath: String)(implicit m: Manifest[T]): Option[T] = {
    implicit val formats: Formats = DefaultFormats + new JKeySerializer
    var json_data: String = null
    if (JsonFilePath.startsWith("s3://")) {
      val CONFIG_URI= new AmazonS3URI(JsonFilePath)
      val s3Object: S3Object = amazonS3Client.getObject(CONFIG_URI.getBucket, CONFIG_URI.getKey)
      json_data = IOUtils.toString(s3Object.getObjectContent)
    } else {
      json_data = scala.io.Source.fromFile(JsonFilePath).mkString
    }
    val jsonobj: JValue = json.parse(json_data)
    Option(jsonobj.extract[T])
  }

/* def getBatchConfigs(s3BucketPath: String, JsonFileName: String): JSONArray = {
    val s3Object: S3Object = amazonS3Client.getObject(s3BucketPath, JsonFileName)
    val json_data = IOUtils.toString(s3Object.getObjectContent())
    val jsonobj = new JSONObject(json_data)
    val ObjectsList = jsonobj.getJSONArray(BATCH_CONFIGS)
    ObjectsList
  }*/
}
