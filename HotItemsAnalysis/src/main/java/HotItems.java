import entity.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.File;
import java.net.URL;
import java.util.Properties;

public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // UserBehavior.csv 的本地文件路径
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "category", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);




//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9091");
//        properties.setProperty("group.id", "consumer-group");
//        properties.setProperty("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");

//        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer011<String>("zhisheng", new SimpleStringSchema(), properties));
        DataStreamSource<UserBehavior> stream = env.createInput(csvInput,pojoType);
//        stream.map(new MapFunction<String, UserBehavior>() {
//                    @Override
//                    public UserBehavior map(String s) throws Exception {
//                        String[] split=s.split(",");
//                        UserBehavior userBehavior = new UserBehavior();
//                        userBehavior.setUserId(Long.parseLong(split[0]));
//                        userBehavior.setItemId(Long.parseLong(split[1]));
//                        userBehavior.setCategory(Integer.parseInt(split[2]));
//                        userBehavior.setBehavior(split[3]);
//                        userBehavior.setTimestamp(Long.parseLong(split[4]));
//                        return userBehavior;
//                    }
//                })
         stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp()*1000;
            }
        })
                  .filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        }).keyBy(new KeySelector<UserBehavior, Long>() {
             @Override
             public Long getKey(UserBehavior value) throws Exception {
                 return value.itemId;
             }
         }).timeWindow(Time.minutes(60),Time.minutes(5))
                .aggregate(new CountAgg(),new WindowResultFunction())
                .keyBy("windowEnd")
                .process(new TopNHotItems(3))
                .print();


          env.execute("HotItemsAnalysis");

    }
}
