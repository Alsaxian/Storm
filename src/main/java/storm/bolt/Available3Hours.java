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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class Available3Hours extends BaseWindowedBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        Date currentDate = new Date();
        long currentTime = currentDate.getTime();

        long timeStation = 0;
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
            timeStation = Long.valueOf(jsonObject.getString("last_update"));

            if (currentTime - timeStation <= 10800000) {
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
        }

        collector.emit(new Values("Les stations ayant la disponibilité en vélos sur les 3 dernières heures est inférieure à 5"));

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






