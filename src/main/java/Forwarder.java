
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


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author MPFEIFER
 */
public class Forwarder {

    private Properties props = null;
    private KafkaConsumer<String, String> consumer = null;
    private String listenTopic = null;
    private String url = null;
    private String user = null;
    private String pwd = null;
    private final Client httpClient = ClientBuilder.newClient();
    private String usernameAndPassword = null;
    private String authorizationHeaderValue = null;
    private boolean debug = true;

    public Forwarder() {
        init();
    }

    public void init() {
        props = new Properties();
        //props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("bootstrap.servers", "cell-1.streaming.eu-amsterdam-1.oci.oraclecloud.com:9092");
        String authToken = "U.76NGNyadayadar3";
        String tenancyName = "oraseemeadesandbox";
        String username = "oracleidentitycloudservice/marcel.pfeifer@oracle.com";
        String streamPoolId = "ocid1.streampool.oc1.eu-amsterdam-1.amaaaaaaop3l36yardp4hio2jsyadayadaivqf6ddvspenpxjd67a";
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
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        listenTopic = "SODAstream";
        url = "https://n7pmwsc8te8fjty-repodb.adb.eu-frankfurt-1.oraclecloudapps.com/ords/json/soda/latest/SODAkafka";
        user = "JSON";
        pwd = "mypwd##";
        usernameAndPassword = user + ":" + pwd;
        authorizationHeaderValue = "Basic " + java.util.Base64.getEncoder().encodeToString(usernameAndPassword.getBytes());
        debug = true;
    }

    public boolean postToDatabase(String payload) {
        WebTarget target = httpClient.target(url);
        Invocation.Builder invocationBuilder = target
                .request(MediaType.APPLICATION_JSON)
                .header("Authorization", authorizationHeaderValue)
                .accept("application/json");
        Response response = invocationBuilder.post(Entity.json(payload));
        String responseString = null;
        int status = response.getStatus();
        boolean successful = false;
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            successful = true;
            if (response.hasEntity() && debug) {
                System.out.println (String.valueOf(response.readEntity(String.class)));
            }
        } else {
            System.out.println ("Could not post to service: status "+status);
        }
        return successful;

    }

    public static void main(String[] args) {
        Forwarder pusher = new Forwarder();
        pusher.consumer.subscribe(Collections.singletonList(pusher.listenTopic)); //(Arrays.asList(pusher.listenTopic));
        while (true) {
            ConsumerRecords<String, String> records = pusher.consumer.poll(Duration.ofMillis(1000));
            //System.out.println("polling...");
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                pusher.postToDatabase(record.value());
            }
            pusher.consumer.commitSync();
        }
    }
}
