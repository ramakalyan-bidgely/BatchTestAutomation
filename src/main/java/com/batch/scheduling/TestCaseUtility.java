package com.batch.scheduling;

import com.amazonaws.services.s3.AmazonS3URI;
import com.batch.creation.BatchCountValidator;
import com.batch.creation.BatchExecutionWatcher;
import com.batch.creation.DBEntryVerification;
import com.batch.creation.ValidateManifestFile;
import com.batch.utils.*;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;
import com.batch.utils.sql.batch.BatchStepDetails;
import com.batch.utils.sql.batch.MainDataProvider;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.batch.utils.AirflowClient;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import static com.batch.api.common.Constants.InputConfigConstants.LATEST_MODIFIED_TIME;
import static com.batch.api.common.Constants.InputConfigConstants.S3_PREFIX;

class TestCaseUtility {
    List<String> dag_ids;
    boolean test_param = true;
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    String dt = new SimpleDateFormat("yyyy/MM/dd").format(new Date());
    private Integer issueCount = 0;
    ApplicationContext context = new ClassPathXmlApplicationContext("batch-dao.xml");

    BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate)
            context.getBean("BatchJDBCTemplate");
    JerseyClient jerseyClient=JerseyClient.REST_CLIENT;
    AirflowClient  airflowClient=new AirflowClient(jerseyClient,"http://batch-qa-2131963712.us-west-2.elb.amazonaws.com", "bidgely-airflow", "B!d53LyA!r8loW");

    public int expectedBatchesTest(JsonObject batchConfig) {
        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        long intervalInSec = bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();
        Integer maxLookUpDays = bc.getMaxLookUpDays();
        Long dataSizeInbytes = bc.getDataSizeInBytes();

        String manifest_prefix = "batch-manifests/pilot_id=" + pilotId + "/batch_id";
        dt = directoryStructure.equals("PartitionByDate") ? "date=" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) : new SimpleDateFormat("yyyy/MM/dd").format(new Date());
        String DEST = S3_PREFIX + s3Bucket + "/TestAutomation/" + pilotId + "/" + dataSetType + "/" + dt + "/" + getClass().getSimpleName();        //long DataAccumulatedSize = BatchCountValidator.UploadAndAccumulate(Dir, DEST);

        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        String SRC = S3_PREFIX + s3Bucket + "/TestData/" + pilotId + "/" + dataSetType + "/" + getClass().getSimpleName();
        AmazonS3URI SRC_URI = new AmazonS3URI(SRC);
        Reporter.log("Getting latest batch creation time", true);
        Timestamp LatestBatchCreationTime = DBEntryVerification.getLatestBatchCreationTime(pilotId, component);
        VariableCollections.map.put("batch_creation_time", LatestBatchCreationTime);
        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();
        List<Map<String, Object>> latestObjectDetails = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);

        Timestamp latest_modified_time = (Timestamp) (latestObjectDetails.size() > 0 ? latestObjectDetails.get(0).get(LATEST_MODIFIED_TIME) : new Timestamp(new Date().getTime()));


        long DataAccumulatedSize = S3FileTransferHandler.S3toS3TransferFiles(DEST_URI, SRC_URI);
        Reporter.log("Data Transferred at " + Calendar.getInstance().getTime() + ",  Data Accumulated Size ...... " + DataAccumulatedSize, true);
        Integer ExpectedNoOfBatches = BatchCountValidator.getExpectedNoOfBatches(s3Bucket, BucketPrefix + "/" + dt, dataSizeInbytes, maxLookUpDays, latest_modified_time,"Firehose");

        Reporter.log("Expected number of batches : " + ExpectedNoOfBatches, true);

        BatchExecutionWatcher.bewatch(1);
        int issueCount = 0;
        try {
            List<String> GeneratedBatches = BatchCountValidator.getBatchManifestFileList(pilotId, component, s3Bucket, manifest_prefix, LatestBatchCreationTime);
            // now we need to verify the manifest files and check whether the object is present in it or not
            Reporter.log("Number of Batches generated: " + GeneratedBatches, true);
            if (GeneratedBatches.size() != ExpectedNoOfBatches) {
                Reporter.log("generated batches is not equal to expected number of batches", true);
                issueCount++;
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
        List<String> eligibleBatches = batchJDBCTemplate.getEligibleBatchesList(bc.getPilotId(), bc.getComponent(), bc.isNextBatchDependentOnPrev(), bc.getParallelBatchesIfIndependent());
        List<String> batchesInStepDetails = batchJDBCTemplate.listBatchesInStepsWithStatus(bc.getPilotId(), bc.getComponent(), "SCHEDULED");
        List<String> actualBatchIds = new ArrayList();
        List<String> eligibleBatchIds = new ArrayList();
        for (String batch_id : batchesInStepDetails) {//select batchID instead of row
            actualBatchIds.add(batch_id);
        }
        for (String batch_id : eligibleBatches) {
            eligibleBatchIds.add(batch_id);
        }
        //actualBatchIds.equals(eligibleBatchIds);Improvement if required
        if (!eligibleBatches.containsAll(actualBatchIds) && actualBatchIds.containsAll(eligibleBatches)) {
            issueCount++;
            Reporter.log("Wrong batches are Scheduled",true);
        }
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
        return issueCount;
    }

    public int SanityCheckForBatchStepDetailsTable(JsonObject batchConfig) {
        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        long intervalInSec = bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();
        Integer maxLookUpDays = bc.getMaxLookUpDays();
        Long dataSizeInbytes = bc.getDataSizeInBytes();
        List<BatchStepDetails> batchesInStepDetails = batchJDBCTemplate.listBatchesInSteps(pilotId, component, "SCHEDULED");
        int issueCount = 0;
        for (BatchStepDetails batch : batchesInStepDetails) {
            if (batch.getBatch_id() == null) {
                issueCount++;
                Reporter.log("batch_id is null for scheduled batch",true);
            }
            if (batch.getStep() == null) {
                issueCount++;
                Reporter.log("Step is null for scheduled batch",true);
            }
            if (batch.getStatus() == null) {
                issueCount++;
                Reporter.log("Status is null for scheduled batch",true);
            }
            if (batch.getPilot_id() == 0) {
                issueCount++;
                Reporter.log("Pilot_id is null for scheduled batch",true);
            }
            if (batch.getComponent() == null) {
                issueCount++;
                Reporter.log("Component is null for scheduled batch",true);
            }
            if (batch.getDag_id() == null) {
                issueCount++;
                Reporter.log("Dag_id is null for scheduled batch",true);
            }
            if (batch.getTry_number() == 0) {
                issueCount++;
                Reporter.log("Try_number is null for scheduled batch",true);
            }
            if (batch.getDag_run_id() == null) {
                issueCount++;
                Reporter.log("Dag_run_id is null for scheduled batch",true);
            }
            if (batch.getMetadata() == null) {
                issueCount++;
                Reporter.log("Metadata is null for scheduled batch",true);
            }
            if (batch.getPriority() == 0) {
                issueCount++;
                Reporter.log("Priority is null for scheduled batch",true);
            }
            if (batch.getStep_sensor_name() == null) {
                issueCount++;
                Reporter.log("step_sensor_name is null for scheduled batch",true);
            }

        }
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
        return issueCount;
    }
    public int checkAirflowStatusoOfDAGRunTest(JsonObject batchConfig){
        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        long intervalInSec = bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();
        Integer maxLookUpDays = bc.getMaxLookUpDays();
        Long dataSizeInbytes = bc.getDataSizeInBytes();
        List<BatchStepDetails> scheduledBatchesInStepDetails = batchJDBCTemplate.listBatchesInSteps(pilotId, component, "SCHEDULED");
        List<DAGRunInfo> dagRunInfos=null;
        int issueCount=0;
        for(BatchStepDetails batch:scheduledBatchesInStepDetails)
        {
            dagRunInfos.add(airflowClient.getDAGRunInfo(batch.getDag_id(),batch.getDag_run_id()));
        }
        for(DAGRunInfo dagRunInfo:dagRunInfos)
        {
            if(dagRunInfo.getState()!="running")
            {
                Reporter.log("Dag running status is not running for scheduled batches",true);
                issueCount++;
            }
        }
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
        return issueCount;
    }
    public int checkAirflowStatusoOfDAGTest(JsonObject batchConfig){
        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        long intervalInSec = bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();
        Integer maxLookUpDays = bc.getMaxLookUpDays();
        Long dataSizeInbytes = bc.getDataSizeInBytes();
        List<BatchStepDetails> scheduledBatchesInStepDetails = batchJDBCTemplate.listBatchesInSteps(pilotId, component, "SCHEDULED");
        List<DAGInfo> dagInfos=null;
        int issueCount=0;
        for(BatchStepDetails batch:scheduledBatchesInStepDetails)
        {
            dagInfos.add(airflowClient.getDagInfo(batch.getDag_id()));
        }
        for(DAGInfo dagInfo:dagInfos)
        {
            if(!dagInfo.getIs_active())
            {
                Reporter.log("Dag  status is not active for scheduled batches",true);
                issueCount++;
            }
        }
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
        return issueCount;
    }
    public int checkAirflowDAGResponseValues(JsonObject batchConfig)
    {
        Calendar c = Calendar.getInstance();
        Reporter.log(getClass().getSimpleName() + " trigger time -> " + c.getTime(), true);
        InputConfig bc = InputConfigParser.getInputConfig(batchConfig);
        int pilotId = bc.getPilotId();
        String s3Bucket = bc.getBucket();
        String component = bc.getComponent();
        String BucketPrefix = bc.getPrefix();
        String directoryStructure = bc.getDirectoryStructure();
        long intervalInSec = bc.getIntervalInSec();
        String dataSetType = bc.getDatasetType();
        Integer maxLookUpDays = bc.getMaxLookUpDays();
        Long dataSizeInbytes = bc.getDataSizeInBytes();
        List<BatchStepDetails> scheduledBatchesInStepDetails = batchJDBCTemplate.listBatchesInSteps(pilotId, component, "SCHEDULED");
        List<DAGInfo> dagInfos=null;
        int issueCount=0;
        for(BatchStepDetails batch:scheduledBatchesInStepDetails)
        {
            dagInfos.add(airflowClient.getDagInfo(batch.getDag_id()));
        }
        for(DAGInfo dagInfo:dagInfos)
        {
            if(dagInfo.getIs_paused()==true)
            {
                Reporter.log("Dag  status is paused for scheduled batches",true);
                issueCount++;
            }
            if(dagInfo.getDag_id()=="")
            {
                Reporter.log("DAG Id is invalid",true);
                issueCount++;
            }
            if(dagInfo.getRoot_dag_id()=="")
            {
                Reporter.log("Root dagId is invalid",true);
                issueCount++;
            }
            if(dagInfo.getSchedule_interval()!=null)
            {
                Reporter.log("schedule Interval should be null as this dag is need based",true);
                issueCount++;
            }
            if(dagInfo.getFileloc()=="")
            {
                Reporter.log("File location is empty string",true);
                issueCount++;
            }
        }
        Reporter.log(getClass().getSimpleName() + " completed time -> " + c.getTime(), true);
        return issueCount;

    }


}