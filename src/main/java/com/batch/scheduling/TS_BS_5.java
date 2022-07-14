package com.batch.scheduling;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.Test;
import com.batch.utils.InputConfig;
import com.batch.utils.InputConfigParser;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.batch.utils.sql.batch.BatchStepDetails;
import com.batch.utils.sql.batch.MainDataProvider;
import utils.sql.batch.restclient.AirflowClient;
import utils.sql.batch.restclient.JerseyClient;

import java.util.HashSet;
import java.util.List;

public class TC_BS_5 {
    ApplicationContext context = new ClassPathXmlApplicationContext("batch-dao.xml");

    BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate)
            context.getBean("BatchJDBCTemplate");
    @Test(dataProvider = "input-data-provider", dataProviderClass = MainDataProvider.class)
    public void validate(String data) {
        String config_file_path=data+"\\"+"meter_batch_config.json";
        InputConfigParser inputConfigParser = new InputConfigParser();
        JsonObject jsonobj = inputConfigParser.getBatchConfig(config_file_path);
        JsonArray jsonArr = jsonobj.getAsJsonArray("batchConfigs");
        JsonObject jsonObject=jsonArr.get(0).getAsJsonObject();
        InputConfig config = InputConfigParser.getInputConfig(jsonObject);
        List<BatchStepDetails> batchesInStepDetails = batchJDBCTemplate.listBatchesInStepsWithStatus(config.getPilotId(),"SCHEDULED");
        Boolean test_param = false;
        JerseyClient jerseyClient=JerseyClient.REST_CLIENT;
        AirflowClient  airflowClient=new AirflowClient(jerseyClient,"http://batch-qa-2131963712.us-west-2.elb.amazonaws.com", "bidgely-airflow", "B!d53LyA!r8loW");
        HashSet<String> dag_ids
                = new HashSet<String>();
        for(BatchStepDetails batch:batchesInStepDetails){
            dag_ids.add(batch.getDag_id());
        }

        for(String dag:dag_ids){
            if(airflowClient.getDagInfo(dag)==null){
                test_param=true;
                Reporter.log("corresponding dag not present for scheduled batches");
            }else{
                if(((Object) airflowClient.getDagInfo(dag).getDag_id()).getClass().getName()!="java.lang.String")
                {
                    test_param=true;
                    Reporter.log("dagId is not string");
                }
                if(((Object) airflowClient.getDagInfo(dag).getIs_active()).getClass().getName()!="java.lang.Boolean")
                {
                    test_param=true;
                    Reporter.log("isActive is not Boolean");
                }
                if(((Object) airflowClient.getDagInfo(dag).getIs_paused()).getClass().getName()!="java.lang.Boolean")
                {
                    test_param=true;
                    Reporter.log("isPaused is not Boolean");
                }
                if(((Object) airflowClient.getDagInfo(dag).getIs_subdag()).getClass().getName()!="java.lang.Boolean")
                {
                    test_param=true;
                    Reporter.log("isSubdag is not Boolean");
                }
                if(((Object) airflowClient.getDagInfo(dag).getFileloc()).getClass().getName()!="java.lang.String")
                {
                    test_param=true;
                    Reporter.log("Fileloc is not string");
                }
            }
        };
        Assert.assertFalse(test_param);
    }
}