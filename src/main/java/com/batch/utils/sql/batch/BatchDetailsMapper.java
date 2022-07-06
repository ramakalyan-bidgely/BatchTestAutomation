package com.batch.utils.sql.batch;

/**
 * @author Rama Kalyan
 */


import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BatchDetailsMapper implements RowMapper<BatchDetails> {
    public BatchDetails mapRow(ResultSet rs, int rowNum) throws SQLException {

        BatchDetails BatchDetails = new BatchDetails();
        BatchDetails.setBatch_id(rs.getString("batch_id"));
        BatchDetails.setPilot_id(rs.getInt("pilot_id"));
        BatchDetails.setComponent(rs.getString("component"));
        BatchDetails.setBatch_creation_time(rs.getTimestamp("batch_creation_time"));
        BatchDetails.setUpdated_at(rs.getTimestamp("updated_at"));
        BatchDetails.setLatest_modified_key(rs.getString("latest_modified_key"));
        BatchDetails.setlatest_modified_time(rs.getTimestamp("latest_modified_time"));
        BatchDetails.setBatch_creation_type(rs.getString("batch_creation_type"));

        return BatchDetails;
    }
}