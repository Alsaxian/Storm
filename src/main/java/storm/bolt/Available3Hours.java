package storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;


public class Available3Hours extends BaseWindowedBolt {
    private static final long serialVersionUID = 4262387370788107343L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        int availableBikes = 0;
        String nameStation = "";
        Boolean status = false;
        Map<String, Boolean> listStations = new HashMap<>();

        for (Tuple t : inputWindow.get()) {
            String n = t.getValueByField("json").toString();

            JsonReader jsonReader = Json.createReader(new StringReader(n));
            JsonObject jsonObject = jsonReader.readObject();

            nameStation = jsonObject.getString("name");
            availableBikes = jsonObject.getInt("available_bikes");

            if (listStations.containsKey(nameStation)) {
                status = listStations.get(nameStation);

                if (status) {
                    if (availableBikes >= 5) {
                        listStations.put(nameStation, false);
                    }
                }
            } else {
                if (availableBikes < 5)
                    listStations.put(nameStation, true);
                else
                    listStations.put(nameStation, false);
            }
        }

        //JsonArray jsonArray = Json.createArrayBuilder();

        collector.emit(new Values("La disponibilité en vélos sur les 3 dernières heures est inférieure à 5"));

        for (String name : listStations.keySet()) {
            if (listStations.get(name)) {
                collector.emit(new Values(name));
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}






