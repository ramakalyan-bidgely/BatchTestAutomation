package com.batch.utils.sql.batch;

import org.springframework.context.annotation.Bean;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.List;


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

    public Timestamp getLatestBatchCreationTime(Integer pilotId, String component) {

        String SQL = "select batch_creation_time from batch_details where pilot_id= ? and component= ?  order by batch_creation_time desc limit 1";
        try {
            Timestamp batchCreationTime = jdbcTemplateObject.queryForObject(SQL, new Object[]{pilotId, component}, Timestamp.class);
            System.out.println("Batch Creation Time : " + batchCreationTime);
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
  /*  public static List<BatchStepDetails> getBatchInSteps(String batch_id) {
        String SQL = "select * from batch_step_details where batch_id=?";
        List <BatchStepDetails> batches=jdbcTemplateObject.query(SQL,new Object[] { batch_id},new BatchStepDetailsMapper());
        return batches;
    }
    public static List<BatchStepDetails> listBatchesInSteps() {
        String SQL = "select * from batch_step_details";
        List <BatchStepDetails> batches= jdbcTemplateObject.query(SQL, new BatchStepDetailsMapper());
        return batches;
    }
*/
}

