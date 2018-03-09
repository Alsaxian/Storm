package storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.Map;

public class AvailableBolt implements IRichBolt {

	private OutputCollector collector;

	public AvailableBolt() {
	}
	
	@Override
    public void execute(Tuple t) {
        String n = t.getValueByField("json").toString();

        JsonReader jsonReader = Json.createReader(new StringReader(n));
        JsonObject jsonObject = jsonReader.readObject();

        int availableBikeStands = jsonObject.getInt("available_bike_stands");
        int availableBikes = jsonObject.getInt("available_bikes");
        String type = "";

        if (availableBikes == 0 && availableBikeStands == 0) {
            type = "Il n'y a plus de vélos et de place disponible";
            collector.emit(new Values(n, type));
            return;
        }

        if (availableBikes == 0) {
            type = "Il n'y a plus de vélos";
            collector.emit(new Values(n, type));
            return;
        }

        if (availableBikeStands == 0) {
            type = "Il n'y a plus de place disponible";
            collector.emit(new Values(n, type));
            return;
        }
    }

	/* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("json", "type"));
    }


    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#getComponentConfiguration()
     */
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IBasicBolt#cleanup()
     */
    public void cleanup() {

    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
     */
    @SuppressWarnings("rawtypes")
    public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
}