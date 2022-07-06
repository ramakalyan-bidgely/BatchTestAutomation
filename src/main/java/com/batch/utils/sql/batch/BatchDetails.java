package com.batch.utils.sql.batch;

import java.sql.Timestamp;

public class BatchDetails {
    private static Timestamp latest_modified_time;
    private String batch_id;
    private int pilot_id;
    private String component;
    private Timestamp updated_at;
    private Timestamp batch_creation_time;
    private String latest_modified_key;
    private String batch_creation_type;

    //------------------------------------
    public static Timestamp getLatest_modified_time() {
        return latest_modified_time;
    }

    public Timestamp getUpdated_at() {
        return updated_at;
    }

    public void setUpdated_at(Timestamp updated_at) {
        this.updated_at = updated_at;
    }

    public void setlatest_modified_time(Timestamp latest_modified_time) {
        BatchDetails.latest_modified_time = latest_modified_time;
    }

    public String getBatch_id() {
        return batch_id;
    }

    public void setBatch_id(String batch_id) {
        this.batch_id = batch_id;
    }

    public int getPilot_id() {
        return pilot_id;
    }

    public void setPilot_id(int pilot_id) {
        this.pilot_id = pilot_id;
    }

    public String getLatest_modified_key() {
        return latest_modified_key;
    }

    public void setLatest_modified_key(String latest_modified_key) {
        this.latest_modified_key = latest_modified_key;
    }

    public String getBatch_creation_type() {
        return batch_creation_type;
    }

    public void setBatch_creation_type(String batch_creation_type) {
        this.batch_creation_type = batch_creation_type;
    }

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public Timestamp getBatch_creation_time() {
        return batch_creation_time;
    }

    public void setBatch_creation_time(Timestamp batch_creation_time) {
        this.batch_creation_time = batch_creation_time;
    }


    //------------------------------------
}
