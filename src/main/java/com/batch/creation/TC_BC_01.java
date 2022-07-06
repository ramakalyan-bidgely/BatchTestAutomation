package com.batch.creation;


/**
 * @author Rama Kalyan
 */


import com.amazonaws.services.s3.AmazonS3Client;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.sql.batch.MainDataProvider;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;

import java.util.logging.Logger;


@Test()
public class TC_BC_01 {

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    private Integer issueCount = 0;

    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)

    public void validate(JsonObject batchConfig) {

        InputConfigParser ConfigParser = new InputConfigParser();
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);

        int pilotId = bc.getPilotId();
        String s3bucket = bc.getBucket();
        String component = bc.getComponent();
        String prefix = bc.getPrefix();


        Reporter.log("Validating Batch Creation components for pilot : {}", pilotId);

        AmazonS3Client amazons3Client = new AmazonS3Client();
        Boolean isBucketAvailable = amazons3Client.doesBucketExistV2(s3bucket);
        Assert.assertEquals(isBucketAvailable, true);

    /*  boolean isPrefixAvailable = false;
        isPrefixAvailable = amazons3Client.doesBucketExist(s3bucket + "/" + prefix);*/
        Long dataSizeInBytes = bc.getDataSizeInBytes();
        Boolean isDataSizeConfigured = dataSizeInBytes > 0;

        long intervalInSec = bc.getIntervalInSec();
        Boolean isIntervalInSecConfigured = intervalInSec > 0;

        String datasetType = "raw_consumption_data";
        Assert.assertEquals(datasetType, "raw_consumption_data");

        int maxLookupDays = bc.getMaxLookUpDays();
        boolean isMaxLookupDaysConfigured = maxLookupDays > 0;


        if (isBucketAvailable && isDataSizeConfigured && isIntervalInSecConfigured && isMaxLookupDaysConfigured) {
            issueCount = 0;
//            logger.log("All inputs are valid for {} - {} - Batch Creation - Test case Passed !", pilotId, component);

        } else if (!isBucketAvailable) {
            issueCount++;
            System.out.println(issueCount);
            //          logger.log("Configured Bucket :{} is invalid  or not available- Test case failed !", s3bucket);
            //break

        } else if (!isDataSizeConfigured) {
            issueCount++;
            System.out.println(issueCount);
            //      log("Configured Data size {} Bytes is invalid or not properly configured- Test case failed !", dataSizeInBytes);
            //break
        } else if (!isIntervalInSecConfigured) {
            issueCount++;
            System.out.println(issueCount);
            //    Reporter.log("Configured Interval time span {} sec is invalid or not properly configured- Test case failed !", intervalInSec);
            //break
        } else if (!isMaxLookupDaysConfigured) {
            issueCount++;
            System.out.println(issueCount);
            //    Reporter.log("Configured Interval time span {} sec is invalid or not properly configured- Test case failed !", intervalInSec);
            //break
        }
        //log("Validating Batch Creation components for pilot : {} and component: {} ", pilotId, component);
        Assert.assertEquals(issueCount, 0);
    }
}

