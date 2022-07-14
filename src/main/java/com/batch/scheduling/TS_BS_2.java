package com.batch.scheduling;

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

import java.util.List;
import java.util.logging.Logger;
import java.util.HashSet;
import utils.sql.batch.restclient.AirflowClient;


public class TC_BS_2 {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());
    List<String> dag_ids;
    boolean test_param=true;
    ApplicationContext context = new ClassPathXmlApplicationContext("batch-dao.xml");

    BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate)
            context.getBean("BatchJDBCTemplate");
    @Test(dataProvider = "input-data-provider",dataProviderClass= MainDataProvider.class)
    public void validate(String data) {
        String config_file_path=data+"\\"+"meter_batch_config.json";
        InputConfigParser inputConfigParser = new InputConfigParser();
        JsonObject jsonobj = inputConfigParser.getBatchConfig(config_file_path);
        JsonArray jsonArr = jsonobj.getAsJsonArray("batchConfigs");
        JsonObject jsonObject=jsonArr.get(0).getAsJsonObject();
        InputConfig config = InputConfigParser.getInputConfig(jsonObject);
        List<BatchStepDetails> batchesInStepDetails = batchJDBCTemplate.listBatchesInSteps(config.getPilotId());

        for(BatchStepDetails batch:batchesInStepDetails){
            if(batch.getBatch_id()==null){
                test_param=false;
                Reporter.log("batch_id is null for scheduled batch");
            }
            if(batch.getStep()==null){
                test_param=false;
                Reporter.log("Step is null for scheduled batch");
            }
            if(batch.getStatus()==null){
                test_param=false;
                Reporter.log("Status is null for scheduled batch");
            }
            if(batch.getPilot_id()==0){
                test_param=false;
                Reporter.log("Pilot_id is null for scheduled batch");
            }
            if(batch.getComponent()==null){
                test_param=false;
                Reporter.log("Component is null for scheduled batch");
            }
            if(batch.getDag_id()==null){
                test_param=false;
                Reporter.log("Dag_id is null for scheduled batch");
            }
            if(batch.getTry_number()==0){
                test_param=false;
                Reporter.log("Try_number is null for scheduled batch");
            }
            if(batch.getDag_run_id()==null){
                test_param=false;
                Reporter.log("Dag_run_id is null for scheduled batch");
            }
            if(batch.getError_message()==null){
                test_param=false;
                Reporter.log("Error_message is null for scheduled batch");
            }
            if(batch.getMetadata()==null){
                test_param=false;
                Reporter.log("Metadata is null for scheduled batch");
            }
            if(batch.getPriority()==0){
                test_param=false;
                Reporter.log("Priority is null for scheduled batch");
            }
            if(batch.getStep_sensor_name()==null){
                test_param=false;
                Reporter.log("step_sensor_name is null for scheduled batch");
            }

        }

//        for(String dag:hashSetBatches){
//            if(AirflowClient.getDagInfo(dag)==null){
//                test_param=false;
//                Reporter.log("corresponding dag not present for scheduled batches");
//            }
//        };
        Assert.assertTrue(test_param);
    }
}
