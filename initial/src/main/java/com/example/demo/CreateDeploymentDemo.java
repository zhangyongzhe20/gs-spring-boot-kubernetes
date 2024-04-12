package com.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;

import javax.annotation.Resource;


public class CreateDeploymentDemo {
    private static CoreV1Api COREV1_API;
    private static final String DEFAULT_NAME_SPACE = "di-datasuite-scheduler-k8s-test";
    private static final Integer TIME_OUT_VALUE = 180;
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDeploymentDemo.class);

    private static RestTemplate restTemplate;
    public static void main(String[] args) throws IOException, ApiException {
        String root = System.getProperty("user.dir");
        String filePath = new ClassPathResource("/" + root + "/initial/src/main/resources/deployment_shell.yaml").getPath();
        System.out.println(filePath);
        String serverName = "shell5";
        String jsonDeploymentStr = convertYamlToJson(filePath, serverName);


        // shopee k8s
        String kubeConfigPath = System.getenv("HOME") + "/kube_shopee/config";
        ApiClient client =
                ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        AppsV1Api api = new AppsV1Api();
        api.setApiClient(client);
        Configuration.setDefaultApiClient(client);
        COREV1_API = new CoreV1Api();
        //read deploy yaml file
        V1Deployment body =
                Configuration.getDefaultApiClient()
                        .getJSON()
                        .deserialize(jsonDeploymentStr, V1Deployment.class);

        // create a deployment
        V1Deployment deploy1 =
                api.createNamespacedDeployment(DEFAULT_NAME_SPACE, body, null, null, null, null);
        V1ObjectMeta metadata = deploy1.getMetadata();
        LOGGER.info("deploy name: {}, label: {}", metadata.getName(), metadata.getLabels());

        // query app running status
        List<V1PodStatus> podsStatus = getPodsStatus(serverName);
        V1PodStatus v1PodStatus = podsStatus.get(0);
        LOGGER.info("pod phase status: {}, podIp: {}, hostIp: {},  all: {}", v1PodStatus.getPhase(), v1PodStatus.getPodIP(), v1PodStatus.getHostIP(),  v1PodStatus.toString());



        // get service logs
        List<String> pods = getPods(serverName);
        String firstPod = pods.get(0);
        printLog(DEFAULT_NAME_SPACE, firstPod);

        // get execution output by index
        // todo: podip may not be ready
        String podIp = v1PodStatus.getPodIP();
        int podPort = 8080;
        int startIndex = 0;
        Integer endIndex = 10000; //pull all the current log
        String executionOutput = getOutputByIndex(podIp, podPort, startIndex, endIndex);
        LOGGER.info("pull log by index: {}", executionOutput);

        // delete deployment
        V1Status v1Status = api.deleteNamespacedDeployment(metadata.getName(), DEFAULT_NAME_SPACE, null, null, null, null, null, null);
        LOGGER.info("delete status: {}, msg: {}", v1Status.getStatus(), v1Status.getMessage());

        //扩容
    }

    private static String getOutputByIndex(String podIp, int podPort, int startIndex, Integer endIndex) {
        restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        HttpEntity httpEntity = new HttpEntity(headers);
        ResponseEntity<String> responseEntity;
        String url = String.format("http://%s:%s", podIp, podPort);
        responseEntity =
                restTemplate.exchange(
                        url
                                + "?start="
                                + startIndex
                                + "&end="
                                +endIndex,
                        HttpMethod.GET,
                        httpEntity,
                        String.class);
        return responseEntity.getBody();
    }

    public static List<String> getPods(String label) throws ApiException {
        V1PodList v1podList =
                COREV1_API.listNamespacedPod(DEFAULT_NAME_SPACE, null, null, null, null, null, null, null, null,  null, null);

        List<String> podList =
                v1podList.getItems().stream()
                        .filter(v1Pod -> v1Pod.getMetadata().getLabels().containsValue(label))
                        .map(v1Pod -> v1Pod.getMetadata().getName())
                        .collect(Collectors.toList());
        return podList;
    }

    public static List<V1PodStatus> getPodsStatus(String label) throws ApiException {
        V1PodList v1podList =
                COREV1_API.listNamespacedPod(DEFAULT_NAME_SPACE, null, null, null, null, null, null, null, null,  null, null);

        List<V1PodStatus> podList =
                v1podList.getItems().stream()
                        .filter(v1Pod -> v1Pod.getMetadata().getLabels().containsValue(label))
                        .map(v1Pod -> v1Pod.getStatus())
                        .collect(Collectors.toList());
        return podList;
    }

    public static void printLog(String namespace, String podName) throws ApiException {
        // https://github.com/kubernetes-client/java/blob/master/kubernetes/docs/CoreV1Api.md#readNamespacedPodLog
        String readNamespacedPodLog =
                COREV1_API.readNamespacedPodLog(
                        podName,
                        namespace,
                        null,
                        Boolean.FALSE,
                        null,
                        Integer.MAX_VALUE,
                        null,
                        Boolean.FALSE,
                        Integer.MAX_VALUE,
                        null,
                        Boolean.FALSE);
        LOGGER.info("read pod log: {}", readNamespacedPodLog);
    }

    static String convertYamlToJson(String path, String appName) throws IOException {
        Yaml yaml = new Yaml();
        String deploymentStr = FileUtils.readFileToString(new File(path));
        // variable replacement
        String finalDeployStr = customizedDeploymentFile(deploymentStr, appName);
        Map<Object, Object> obj = yaml.load(finalDeployStr);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(obj);
    }

    private static String customizedDeploymentFile(String deploymentStr, String appName) {
        Map<String, Object> mixedVariableMap = new HashMap<>();
        mixedVariableMap.put("APP_NAME", appName);
        mixedVariableMap.put("OUTPUT_S3_PATH", "k8s/shellOutput");
        mixedVariableMap.put("SOURCE_CODE_S3_PATH", "k8s/shellCode");
        String finalDeployStr = replacePlaceholderWithNested(deploymentStr, mixedVariableMap);
        return finalDeployStr;
    }

    public static String replacePlaceholderWithNested(String template, Map<String, Object> values) {
        if (StringUtils.isEmpty(template) || values == null || values.isEmpty()) {
            return template;
        } else {
            StringSubstitutor stringSubstitutor = new StringSubstitutor();
            stringSubstitutor.setEnableSubstitutionInVariables(true);
            return StringSubstitutor.replace(template, values);
        }
    }
}
