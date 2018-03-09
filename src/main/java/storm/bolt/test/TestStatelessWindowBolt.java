package storm.bolt.test;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.Map;


public class TestStatelessWindowBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 4262387370788107343L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {

        int cpt = 0;

        for (Tuple t : inputWindow.get()) {
            cpt++;
        }


        JsonObjectBuilder r = Json.createObjectBuilder();
        r.add("test", "statelessWithWindow");
        r.add("nbTuplesInWindow", cpt);
        JsonObject row = r.build();

        collector.emit(inputWindow.get(), new Values(row.toString()));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}






