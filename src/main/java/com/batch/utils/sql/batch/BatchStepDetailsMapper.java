package com.batch.utils.sql.batch;

import org.springframework.jdbc.core.RowMapper;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BatchStepDetailsMapper implements RowMapper<BatchStepDetails> {
    public BatchStepDetails mapRow(ResultSet rs, int rowNum) throws SQLException {
        BatchStepDetails batchStepDetails = new BatchStepDetails();
        batchStepDetails.setBatch_id(rs.getString("batch_id"));
        batchStepDetails.setPilot_id(rs.getInt("pilot_id"));
        batchStepDetails.setComponent(rs.getString("component"));
        batchStepDetails.setStep(rs.getString("step"));
        batchStepDetails.setUpdated_at(rs.getTimestamp("updated_at"));
        batchStepDetails.setStatus(rs.getString("status"));
        batchStepDetails.setDag_id(rs.getString("dag_id"));
        batchStepDetails.setTry_number(rs.getInt("try_number"));
        batchStepDetails.setDag_run_id(rs.getString("dag_run_id"));
        batchStepDetails.setError_message(rs.getString("error_message"));
        batchStepDetails.setMetadata(rs.getString("metadata"));
        batchStepDetails.setPriority(rs.getInt("priority"));
        batchStepDetails.setStep_sensor_name(rs.getString("step_sensor_name"));

        return batchStepDetails;
    }
}
