package com.batch.creation.UserSeg;


/**
 * @autor rama kalyan
 */


import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

import java.util.logging.Logger;


@Test()
public class TC_BC_01 {

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test

    public void validate() {
        InputConfigParser ConfigParser = new InputConfigParser();

        String jsonFilePath = "s3://bidgely-adhoc-batch-qa/TestAutomation/batchConfig.json";
        JsonObject batchConfig = InputConfigParser.getBatchConfig(jsonFilePath);

        JsonObject batchconfigs = batchConfig.get("batchConfigs").getAsJsonArray().get(0).getAsJsonObject();


        // JsonObject value =InputConfigParser.getBatchInputs(batchConfig.get("batchConfigs").getAsString());
        //System.out.println(value);

        InputConfig bc = InputConfigParser.getInputConfig(batchconfigs);

        int pilotId = bc.getPilotId();
        String s3bucket = bc.getBucket();
        String component = bc.getComponent();
        String prefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        String datasetType = bc.getDatasetType();
        String dataFormat = bc.getDataFormat();
        String compressionFormat = bc.getCompressionFormat();
        boolean skipSucceededTasksOnRetry = bc.isSkipSucceededTasksOnRetry();
        boolean isNextBatchDependentOnPrev = bc.isNextBatchDependentOnPrev();
        int parallelBatchesIfIndependent = bc.getParallelBatchesIfIndependent();
        int maxTries = bc.getMaxTries();



        Reporter.log("Validating Batch Creation components for pilot : {}", pilotId);

        AmazonS3Client amazons3Client = new AmazonS3Client();
        Boolean isBucketAvailable = amazons3Client.doesBucketExistV2(s3bucket);


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
        }  if (!isBucketAvailable) {
            issueCount++;
            System.out.println(issueCount);


        }  if (!isDataSizeConfigured) {
            issueCount++;
            System.out.println(issueCount);
        }  if (!isIntervalInSecConfigured) {
            issueCount++;
            System.out.println(issueCount);
        } if (!isMaxLookupDaysConfigured) {
            issueCount++;
            System.out.println(issueCount);
        }
        if (!directoryStructure.equals("Firehose")){
            issueCount++;
            System.out.println(issueCount);

        }
        if (!skipSucceededTasksOnRetry){
            issueCount++;
            System.out.println(issueCount);

        }
        if (!isNextBatchDependentOnPrev){
            issueCount++;
            System.out.println(issueCount);
        }
        if (parallelBatchesIfIndependent!=2){
            issueCount++;
            System.out.println(issueCount);

        }
        if (maxTries!=2){
            issueCount++;
            System.out.println(issueCount);

        }
        if (!compressionFormat.equals("snappy")){
            issueCount++;
            System.out.println(issueCount);

        }
        if (!dataFormat.equals("parquet")){
            issueCount++;
            System.out.println(issueCount);

        }
        if (!component.equals("batch_invoice_ingestion")){
            issueCount++;
            System.out.println(issueCount);

        }
        Assert.assertEquals(issueCount, 0);
    }
}

