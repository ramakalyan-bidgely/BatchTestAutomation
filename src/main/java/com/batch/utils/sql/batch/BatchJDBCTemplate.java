package com.batch.utils.sql.batch;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


@Component
public class BatchJDBCTemplate {
    private static JdbcTemplate jdbcTemplateObject;
    private DataSource dataSource;

    public BatchJDBCTemplate(JdbcTemplate jdbcTemplateObject) {
        BatchJDBCTemplate.jdbcTemplateObject = jdbcTemplateObject;
    }

    public BatchJDBCTemplate() {

    }


    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        jdbcTemplateObject = new JdbcTemplate(dataSource);
    }

    public List<BatchDetails> getBatch(String batch_id) {
        String SQL = "select * from batch_details where batch_id=?";
        List<BatchDetails> batches = jdbcTemplateObject.query(SQL, new Object[]{batch_id}, new BatchDetailsMapper());
        return batches;
    }

    public List<BatchDetails> listBatches(Integer pilotId) {
        String SQL = "select * from batch_details where pilot_id=" + pilotId;
        List<BatchDetails> batches = jdbcTemplateObject.query(SQL, new BatchDetailsMapper());
        return batches;
    }

    public List<Map<String, Object>> getLatestObjectDetails(Integer pilotId, String component) {

        String SQL = "select latest_modified_key, latest_modified_time from batch_details where pilot_id= ? and component= ?  order by batch_creation_time desc limit 1";
        try {
            //Timestamp latestModifiedTime = jdbcTemplateObject.queryForObject(SQL, new Object[]{pilotId, component}, Timestamp.class);
            List<Map<String, Object>> latestObjectDetails = jdbcTemplateObject.queryForList(SQL, new Object[]{pilotId, component});
            return latestObjectDetails;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public Timestamp getLatestBatchCreationTime(Integer pilotId, String component) {

        String SQL = "select batch_creation_time from batch_details where pilot_id= ? and component= ?  order by batch_creation_time desc limit 1";
        try {
            Timestamp batchCreationTime = jdbcTemplateObject.queryForObject(SQL, new Object[]{pilotId, component}, Timestamp.class);
            return batchCreationTime;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<BatchDetails> listBatchIds(Integer pilotId) {
        String SQL = "select batch_id from batch_details where pilot_id=" + pilotId;
        List<BatchDetails> batchIds = jdbcTemplateObject.query(SQL, new BatchDetailsMapper());
        return batchIds;
    }
//    public static List<BatchStepDetails> getBatchInSteps(String batch_id) {
//        String SQL = "select * from batch_step_details where batch_id=?";
//        List <BatchStepDetails> batches=jdbcTemplateObject.query(SQL,new Object[] { batch_id},new BatchStepDetailsMapper());
//        return batches;
//    }
//    public static List<BatchStepDetails> listBatchesInSteps() {
//        String SQL = "select * from batch_step_details";
//        List <BatchStepDetails> batches= jdbcTemplateObject.query(SQL, new BatchStepDetailsMapper());
//        return batches;
//    }
    public static List<BatchStepDetails> listBatchesInSteps(Integer pilotId,String component,String status) {
        String SQL = "select * from batch_step_details where pilot_id=? and component=? and status=?;";
        List <BatchStepDetails> batches= jdbcTemplateObject.query(SQL,new Object[] {pilotId,component,status}, new BatchStepDetailsMapper());
        return batches;
    }
    public static List<String> listBatchesInStepsWithStatus(Integer pilotId,String component,String status) {
        String SQL = "select * from batch_step_details where pilot_id=? and component=? and status=?";
        List <BatchStepDetails> batches= jdbcTemplateObject.query(SQL,new Object[] {pilotId,component,status}, new BatchStepDetailsMapper());
        List<String> batchIds=new ArrayList();
        for(BatchStepDetails batch:batches)
        {
            batchIds.add(batch.getBatch_id());
        }
        return batchIds;
    }
    public static List<String> getEligibleBatchesList(Integer pilotId,String component,Boolean isNextBatchDependentOnPrev,Integer parallelBatchesIfIndependent) {
        String SQL ="select * from batch_details where pilot_id=? and component=? and batch_id not in (select batch_id from batch_step_details where pilot_id=? and component=?)";
        List<BatchDetails> batches = jdbcTemplateObject.query(SQL,new Object[] {pilotId,component,pilotId,component},new BatchDetailsMapper());
        List<String> batchIds=new ArrayList();
        if(isNextBatchDependentOnPrev==true)
        {
            BatchDetails batch=batches.get(0);
            batchIds.add(batch.getBatch_id());
        }else{
            for (int i=0;i<parallelBatchesIfIndependent;i++)
            {
                BatchDetails batch=batches.get(i);
                batchIds.add(batch.getBatch_id());
            }
        }
        return batchIds;
    }

}

