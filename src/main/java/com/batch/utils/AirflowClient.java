package com.batch.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.JsonElement;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.Base64;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import com.sun.jersey.api.client.ClientResponse;



import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AirflowClient {
    public static final Gson GSON_WITH_PRETTY_PRINTING = new GsonBuilder().setPrettyPrinting().create();
    private static final Logger logger = Logger.getLogger(AirflowClient.class.getSimpleName());
    private static String GetDagBasicInfoApi = "/api/v1/dags/%s";
    private static String GetDagRun = "/api/v1/dags/%s/dagRuns/%s";
    private Gson GsonWithLowerSnakeCase =
            new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    private static String AuthHeader = "Authorization";
    private static String Basic = "Basic ";
    private static JerseyClient jerseyClient= null;
    private static String webServerUrl = null;
    private String airflowUser = null;
    private String airflowPassword = null;
    private static String oauthStringEnc = null;
    public AirflowClient(JerseyClient jerseyClient, String webServerUrl, String airflowUser, String airflowPassword) {
        this.webServerUrl = webServerUrl;
        this.jerseyClient = jerseyClient;
        this.airflowUser = airflowUser;
        this.airflowPassword = airflowPassword;
        String  oauthString = airflowUser + ":" + airflowPassword;
        oauthStringEnc = new String(Base64.encode(oauthString.getBytes()));
    }
    public static DAGInfo getDagInfo(String dagId){

        URI api = UriBuilder.fromUri(webServerUrl + String.format(GetDagBasicInfoApi, dagId)).build();

        WebResource webResource = jerseyClient.resource(api);
        DAGInfo clientResponse= webResource
                .accept(MediaType.APPLICATION_JSON)
                .header(AuthHeader, Basic + oauthStringEnc)
                .get(DAGInfo.class);
        //String responseString = GSON_WITH_PRETTY_PRINTING.toJson(clientResponse.getEntity(ClientResponse.class));
        //DAGInfo dagInfo=null;
        //dagInfo = clientResponse.getEntity(DAGInfo.class);

        //if (clientResponse.getStatus()!=200) {
        //  logger.log(Level.ALL,"dag not found");
        // return null;
        //}
        return clientResponse;

    }
    public static DAGRunInfo getDAGRunInfo (String dagId,String dagRunId){
        URI api = UriBuilder.fromUri(webServerUrl + String.format(GetDagRun, dagId,dagRunId)).build();

        WebResource webResource = jerseyClient.resource(api);
        DAGRunInfo clientResponse= webResource
                .accept(MediaType.APPLICATION_JSON)
                .header(AuthHeader, Basic + oauthStringEnc)
                .get(DAGRunInfo.class);
        return clientResponse;
    }
}
