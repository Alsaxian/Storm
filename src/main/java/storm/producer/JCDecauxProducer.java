package storm.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import storm.topology.Topology;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class JCDecauxProducer {

    private String apiData;
    private Producer<String, String> producer;

    public JCDecauxProducer(Properties props, String filename) throws IOException {
        producer = new KafkaProducer<>(props);
        //URL url = new URL("https://api.jcdecaux.com/vls/v1/stations?contract=Lyon&apiKey=6a67af183f4ed2f0fd9c7e1d267ac677b04bbd8e");
        //HttpURLConnection httpRequest = (HttpURLConnection) url.openConnection();
        //httpRequest.connect();
        apiData = new Scanner(new File(filename)).useDelimiter("\\Z").next();// httpRequest.getContent().toString();
    }

    public void start() {

        JsonReader jsonReader = Json.createReader(new StringReader(apiData));
        JsonArray stations = jsonReader.readArray();

        for (int i = 0; i < stations.size(); i++) {
            JsonObject station = stations.getJsonObject(i);
            producer.send(new ProducerRecord<String, String>(Topology.topics, Integer.toString(i), station.toString()));
        }

        producer.close();
    }
}
