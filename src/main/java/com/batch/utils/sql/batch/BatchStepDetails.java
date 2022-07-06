package com.batch.utils.sql.batch;
import java.sql.Timestamp;

public class BatchStepDetails {
    private String batch_id;
    private String step;
    private String status;
    private int pilot_id;
    private String component;
    private Timestamp updated_at;
    private String dag_id;
    private int try_number;
    private String dag_run_id;
    private String error_message;
    private String metadata;
    private int priority;
    private String step_sensor_name;

    public String getStep_sensor_name() {
        return step_sensor_name;
    }

    public void setStep_sensor_name(String step_sensor_name) {
        this.step_sensor_name = step_sensor_name;
    }

    public void setBatch_id(String batch_id) {
        this.batch_id = batch_id;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setPilot_id(int pilot_id) {
        this.pilot_id = pilot_id;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void setUpdated_at(Timestamp updated_at) {
        this.updated_at = updated_at;
    }

    public void setDag_id(String dag_id) {
        this.dag_id = dag_id;
    }

    public void setTry_number(int try_number) {
        this.try_number = try_number;
    }

    public void setDag_run_id(String dag_run_id) {
        this.dag_run_id = dag_run_id;
    }

    public void setError_message(String error_message) {
        this.error_message = error_message;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public String getMetadata() {
        return metadata;
    }
    public String getError_message() {
        return error_message;
    }

    public String getDag_run_id() {
        return dag_run_id;
    }

    public int getTry_number() {
        return try_number;
    }

    public String getDag_id() {
        return dag_id;
    }

    public Timestamp getUpdated_at() {
        return updated_at;
    }

    public String getComponent() {
        return component;
    }
    public int getPilot_id() {
        return pilot_id;
    }
    public String getBatch_id() {
        return batch_id;
    }
    public String getStep() {
        return step;
    }

    public String getStatus() {
        return status;
    }
}
