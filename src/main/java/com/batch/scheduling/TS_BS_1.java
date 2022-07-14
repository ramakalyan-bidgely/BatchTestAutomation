package com.batch.scheduling;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.batch.utils.InputConfigParser;
import com.batch.utils.InputConfig;
import com.batch.utils.sql.batch.BatchStepDetails;
import com.batch.utils.sql.batch.MainDataProvider;
import com.batch.utils.sql.batch.BatchJDBCTemplate;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.HashSet;
import com.batch.utils.sql.batch.restclient.AirflowClient;
import com.batch.utils.S3FileTransferHandler;

@Test()
public class TC_BS_1 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    List<String> dag_ids;
    boolean test_param=true;
    ApplicationContext context = new ClassPathXmlApplicationContext("batch-dao.xml");

    BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate)
            context.getBean("BatchJDBCTemplate");
    @Test(dataProvider = "input-data-provider",dataProviderClass= MainDataProvider.class)
    public void validate(String data) {
        List<File> files= S3FileTransferHandler.ListofFiles(data);
        List<String> dataFiles=new ArrayList<>();
        String config_file_path=data+"\\"+"meter_batch_config.json";
        InputConfigParser inputConfigParser = new InputConfigParser();
        JsonObject jsonobj = inputConfigParser.getBatchConfig(config_file_path);
        JsonArray jsonArr = jsonobj.getAsJsonArray("batchConfigs");
        JsonObject jsonObject=jsonArr.get(0).getAsJsonObject();
        for(File file:files)
        {
            dataFiles.add(file.getPath());
        }
        InputConfig config = InputConfigParser.getInputConfig(jsonObject);
//        AmazonS3URI destAddress=null;
//        String sourceDir=data+"\\"+"data";
//        long accumulatedSize=S3FileTransferHandler.TransferFiles(destAddress,sourceDir);
        //List<String> eligibleBatches=BatchJDBCTemplate.getEligibleBatchesList(config.getPilotId(),config.isNextBatchDependentOnPrev(),config.getParallelBatchesIfIndependent());
        List<BatchStepDetails> eligibleBatches=batchJDBCTemplate.listBatchesInSteps(config.getPilotId());
        List<BatchStepDetails> batchesInStepDetails = batchJDBCTemplate.listBatchesInSteps(config.getPilotId());
        List<String> actualBatchIds=new ArrayList();
        List<String> eligibleBatchIds=new ArrayList();
        for(BatchStepDetails x:batchesInStepDetails)
        {
            actualBatchIds.add(x.getBatch_id());
        }
        for(BatchStepDetails y:eligibleBatches)
        {
            eligibleBatchIds.add(y.getBatch_id());
        }

        if(eligibleBatchIds.get(0)==actualBatchIds.get(0))
        {
            test_param=false;
            Reporter.log("Wrong batches are scheduled");
        }
//        for(BatchStepDetails batch:batchesInStepDetails){
//            dag_ids.add(batch.getDag_id());
//        }
//        HashSet<String> hashSetBatches
//                = new HashSet<String>();
//        for(String dag:hashSetBatches){
//            if(AirflowClient.getDagInfo(dag)==null){
//                test_param=false;
//                Reporter.log("corresponding dag not present for scheduled batches");
//            }
//        };
        Assert.assertTrue(test_param);
    }

}

