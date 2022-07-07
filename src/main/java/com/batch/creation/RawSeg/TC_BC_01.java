package com.batch.creation.RawSeg;


/**
 * @autor rama kalyan
 */


import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.ManifestFileParser;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.logging.Logger;


@Test()
public class TC_BC_01 {

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test

    public void validate() throws IOException {

        String jsonFilePath = "./raw_batch_config.json";


        InputConfigParser ConfigParser = new InputConfigParser();

        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);
        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();

        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);
//        InputConfigParser ConfigParser = new InputConfigParser();
//
//        AmazonS3URI batchInputConfigPath = new AmazonS3URI("s3://bidgely-adhoc-dev/10061/rawingestion/raw_batch_config.json");
//
//        AmazonS3Client amazonS3Client = new AmazonS3Client();
//        S3Object batchInputconfig = amazonS3Client.getObject(batchInputConfigPath.getBucket(), batchInputConfigPath.getKey());
//        String batchConfig = ManifestFileParser.displayTextInputStream(batchInputconfig.getObjectContent());
//
//
//        JsonObject batchConfigs = ManifestFileParser.convertingToJsonObject(batchConfig);
//
//        JsonObject bcs = batchConfigs.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();
//
//        InputConfig bc = InputConfigParser.getInputConfig(bcs);

        int pilotId = bc.getPilotId();
        String s3bucket = bc.getBucket();
        String component = bc.getComponent();
        String DataPathPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        String datasetType = bc.getDatasetType();
        String dataFormat = bc.getDataFormat();
        String compressionFormat = bc.getCompressionFormat();
        boolean skipSucceededTasksOnRetry = bc.isSkipSucceededTasksOnRetry();
        boolean isNextBatchDependentOnPrev = bc.isNextBatchDependentOnPrev();
        int parallelBatchesIfIndependent = bc.getParallelBatchesIfIndependent();
        int maxTries = bc.getMaxTries();
        String dagId = bc.getDagId();
        System.out.println("dagId = " + dagId);

        Reporter.log("Validating Batch Creation components for pilot : {}", pilotId);

        AmazonS3Client amazons3Client = new AmazonS3Client();
        boolean isBucketAvailable = amazons3Client.doesBucketExistV2(s3bucket);

        boolean isPrefixAvailable = amazons3Client.doesBucketExistV2(s3bucket + "/" + DataPathPrefix);

        Long dataSizeInBytes = bc.getDataSizeInBytes();
        boolean isDataSizeConfigured = dataSizeInBytes > 0;


        //validating intervalInSec Threshold
        long intervalInSec = bc.getIntervalInSec();
        boolean isIntervalInSecConfigured = intervalInSec > 0;


        //Validating maxLookupDays
        int maxLookupDays = bc.getMaxLookUpDays();

        boolean isMaxLookupDaysConfigured = maxLookupDays > 0;


        if (isBucketAvailable && isDataSizeConfigured && isIntervalInSecConfigured && isMaxLookupDaysConfigured) {
            issueCount = 0;
        }
        if (!isBucketAvailable) {
            issueCount++;
            Reporter.log("Bucket is not available, Verify the Input Configuration file : " + s3bucket);
        }
        if (!isPrefixAvailable) {
            issueCount++;
            Reporter.log("Prefix is not available, Verify the Input Configuration file : " + DataPathPrefix);
        }
        if (!isDataSizeConfigured) {
            issueCount++;
            Reporter.log("Issue in Configured Threshold of DataSizeConfigured : " + dataSizeInBytes);
        }
        if (!isIntervalInSecConfigured) {
            issueCount++;
            Reporter.log("Issue in Configured Threshold of IntervalInSec : " + intervalInSec);
        }
        if (!isMaxLookupDaysConfigured) {
            issueCount++;
            Reporter.log("Invalid number of Max Lookup Days : " + maxLookupDays);
        }

        if (!directoryStructure.equals("Firehose")) {
            issueCount++;
            Reporter.log("Issue in directory Structure : " + directoryStructure);

        }
        /*if (!skipSucceededTasksOnRetry) {
            issueCount++;
            System.out.println("here 7");
            System.out.println(issueCount);

        } if (!isNextBatchDependentOnPrev) {
            issueCount++;
            System.out.println("here 6");
            System.out.println(issueCount);
        }
        if (parallelBatchesIfIndependent != 2) {
            issueCount++;
            System.out.println("here 5");
            System.out.println(issueCount);

        }
        if (maxTries != 2) {
            issueCount++;
            System.out.println("here 4");
            System.out.println(issueCount);

        }*/
        if (!compressionFormat.equals("snappy")) {
            issueCount++;
            Reporter.log("Issue in Compression Format : " + compressionFormat);
        }
        if (!dataFormat.equals("parquet")) {
            issueCount++;
            Reporter.log("Issue in dataFormat  :" + dataFormat);
        }

        if (!dagId.equals("batch_raw_data_ingestion")) {
            issueCount++;
            Reporter.log("Issue in dagId : " + dagId);
        }


        Assert.assertEquals(issueCount, 0);
    }
}

