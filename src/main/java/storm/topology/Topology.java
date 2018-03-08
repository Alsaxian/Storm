package storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import storm.bolt.ExitBolt;
import storm.bolt.SplitBolt;
import storm.producer.JCDecauxProducer;

import java.util.Properties;
import java.util.UUID;

public class Topology {

    private final static String zookeeper = "192.168.73.205:2181";
    private final static String broker = "192.168.73.227:9092";
    public final static String topics = "A-Topics-API";

    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portOUTPUT = 9002;
        String ipmOUTPUT = "225.0." + args[0] + "." + args[1];

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 1024 * 1024 * 4);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        JCDecauxProducer jcDecauxProducer = new JCDecauxProducer(props, args[2]);
        jcDecauxProducer.start();

        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        BrokerHosts hosts = new ZkHosts(broker);
        SpoutConfig spoutConfig = new SpoutConfig (hosts, topics, "/" + topics, UUID.randomUUID().toString());
        spoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        spoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout spout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", spout);
        builder.setBolt("stations-spitter", new SplitBolt(), nbExecutors).shuffleGrouping("kafka-spout");
        //builder.setBolt("disponibilite-counter", new CountBolt(), nbExecutors).shuffleGrouping("stations- spitter");
        builder.setBolt("exit", new ExitBolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("nofilter");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStorm", config, builder.createTopology());
        StormSubmitter.submitTopology("topoProjetGrpA", config, builder.createTopology());
    }
}
