package storm.topology;

import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.storm.topology.base.BaseWindowedBolt;
import storm.bolt.*;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Topology {

    private final static String zookeeper = "192.168.73.205:2181";
    private final static String broker = "192.168.73.227:9092";
    public final static String topics = "A-Topics-API";

    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //JCDecauxProducer jcDecauxProducer = new JCDecauxProducer(props, args[0]);
        //jcDecauxProducer.start();

        Config config = new Config();

        BrokerHosts hosts = new ZkHosts(zookeeper);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topics, "/" + topics, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = OffsetRequest.LatestTime();
        KafkaSpout spout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", spout);
        builder.setBolt("stations-splitter", new SplitBolt(), nbExecutors).shuffleGrouping("kafka-spout");
        builder.setBolt("available-detector", new AvailableBolt(), nbExecutors).shuffleGrouping("stations-splitter");
        builder.setBolt("available-3h", new Available3Hours().withWindow(new BaseWindowedBolt.Duration(30, TimeUnit.SECONDS)), nbExecutors).shuffleGrouping("stations-splitter");
        builder.setBolt("exit", new ExitBolt(), nbExecutors).shuffleGrouping("available-detector");
        builder.setBolt("exit2", new Exit2Bolt(), nbExecutors).shuffleGrouping("available-3h");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStorm", config, builder.createTopology());
        StormSubmitter.submitTopology("topoProjetGrpA", config, builder.createTopology());
    }
}
