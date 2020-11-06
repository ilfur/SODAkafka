/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author MPFEIFER
 */
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.util.Collections;
import javax.ws.rs.core.MediaType;
import org.apache.kafka.clients.producer.ProducerRecord;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author MPFEIFER
 */
public class Producer {

    private Properties props = null;
    private KafkaProducer<String, String> producer = null;
    private String listenTopic = null;
    private String url = null;
    private String user = null;
    private String pwd = null;
    private final Client httpClient = ClientBuilder.newClient();
    private String usernameAndPassword = null;
    private String authorizationHeaderValue = null;
    private boolean debug = true;

    public Producer() {
        init();
    }

    public void init() {
        props = new Properties();
        //props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("bootstrap.servers", "cell-1.streaming.eu-amsterdam-1.oci.oraclecloud.com:9092");
        String authToken = "U.76NGNyadayada}r3";
        String tenancyName = "oraseemeadesandbox";
        String username = "oracleidentitycloudservice/marcel.pfeifer@oracle.com";
        String streamPoolId = "ocid1.streampool.oc1.eu-amsterdam-1.amaaaaaaop3l36yardp4hio2yadayadapivqf6ddvspenpxjd67a";
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "PLAIN");
        props.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + tenancyName + "/"
                + username + "/"
                + streamPoolId + "\" "
                + "password=\""
                + authToken + "\";");
        props.setProperty("group.id", "oracleGroup");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        listenTopic = "SODAstream";
        url = "https://n7pmwsc8te8fjty-repodb.adb.eu-frankfurt-1.oraclecloudapps.com/ords/json/soda/latest/SODAkafka";
        user = "JSON";
        pwd = "mypwd##";
        usernameAndPassword = user + ":" + pwd;
        authorizationHeaderValue = "Basic " + java.util.Base64.getEncoder().encodeToString(usernameAndPassword.getBytes());
        debug = true;
    }

    public static void main(String[] args) {
        Producer pusher = new Producer();
        try {
            pusher.producer.send(new ProducerRecord<String, String>(pusher.listenTopic, "{\"TEST\":\"TEST\"}")).get();
        } catch (Exception ex) {
            System.out.print(ex.getMessage());
            
        }
    }
}
