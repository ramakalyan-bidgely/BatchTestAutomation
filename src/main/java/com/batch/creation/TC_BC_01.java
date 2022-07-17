package com.batch.creation;


/**
 * @autor Rama Kalyan
 */


import com.amazonaws.services.s3.AmazonS3Client;


import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.BATCH_CONFIGS;


@Test()
public class TC_BC_01 {


    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    //@Parameters("batchConfigPath")
    public void validate(JsonObject batchConfig) throws IOException {

        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);


        //JsonObject batchConfig = InputConfigParser.getBatchConfig(batchConfigPath);

        //InputConfig bc = InputConfigParser.getInputConfig(batchConfig.get(BATCH_CONFIGS).getAsJsonArray().get(0).getAsJsonObject());
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);

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

        Reporter.log("Validating Batch Creation components for pilot : " + pilotId, true);

        AmazonS3Client amazons3Client = new AmazonS3Client();
        boolean isBucketAvailable = amazons3Client.doesBucketExistV2(s3bucket);

        //boolean isPrefixAvailable = amazons3Client.doesBucketExistV2(s3bucket + "/" + DataPathPrefix + "/");

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
            Reporter.log("Bucket is not available, Verify the Input Configuration file : " + s3bucket, true);
        }
//        if (!isPrefixAvailable) {
//            issueCount++;
//            Reporter.log("Prefix is not available, Verify the Input Configuration file : " + DataPathPrefix, true);
//        }
        if (!isDataSizeConfigured) {
            issueCount++;
            Reporter.log("Issue in Configured Threshold of DataSizeConfigured : " + dataSizeInBytes, true);
        }
        if (!isIntervalInSecConfigured) {
            issueCount++;
            Reporter.log("Issue in Configured Threshold of IntervalInSec : " + intervalInSec, true);
        }
        if (!isMaxLookupDaysConfigured) {
            issueCount++;
            Reporter.log("Invalid number of Max Lookup Days : " + maxLookupDays, true);
        }

        if (!(directoryStructure.equals("Firehose") || directoryStructure.equals("PartitionByDate"))) {
            issueCount++;
            Reporter.log("Issue in directory Structure : " + directoryStructure, true);

        }

//        if (skipSucceededTasksOnRetry || !skipSucceededTasksOnRetry) {
//            issueCount++;
//            Reporter.log("Invalid value provided for skip Succeeded Tasks On Retry", true);
//        }

       /* if (!skipSucceededTasksOnRetry) {
            issueCount++;
            Reporter.log(issueCount, true);

        }*/
     /*   if (!isNextBatchDependentOnPrev) {
            issueCount++;
            Reporter.log(issueCount, true);
        }*/
        if (parallelBatchesIfIndependent <= 0) {
            issueCount++;
            Reporter.log("Invalid number of ", true);

        }
        if (maxTries <= 0) {
            issueCount++;
            Reporter.log("Invalid maxTries count has been provided -> " + maxTries, true);
        }
        if (!compressionFormat.equals("snappy")) {
            issueCount++;
            Reporter.log("Compression format is invalid : " + compressionFormat);
        }
        if (!dataFormat.equals("parquet")) {
            issueCount++;
            Reporter.log("data Format is invalid  :" + dataFormat);
        }

        if (!dagId.equals("batch_raw_data_ingestion")) {
            issueCount++;
            Reporter.log("Issue in dagId : " + dagId);
        }

        Assert.assertEquals(issueCount, 0);
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
    }
}

