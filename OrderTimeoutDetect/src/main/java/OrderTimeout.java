import entity.OrderEvent;
import entity.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OrderTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


//        private final List<OrderEvent> orderEventList = Arrays.asList(
//                new OrderEvent("1", "create"),
//                new OrderEvent("2", "create"),
//                new OrderEvent("2", "pay")
//        );

        ArrayList<OrderEvent> data = new ArrayList<>();
        data.add(new OrderEvent(new Long(1),  "create", new Long(1558430842)));
        data.add(new OrderEvent(new Long(2),  "create", new Long(1558430843)));
        data.add(new OrderEvent(new Long(2), "pay", new Long(1558430844)));

        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.fromCollection(data).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent orderEvent) {
                return orderEvent.eventTime * 1000;
            }
        });


        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("begin").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                return value.eventType.equals("create");
            }
        }).followedBy("follow").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                return value.eventType.equals("pay");
            }
        }).within(Time.minutes(15));


        // 定义一个输出标签 
        OutputTag<OrderResult> orderTimeoutOutput = new OutputTag<OrderResult>("orderTimeout"){};

        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern);


        SingleOutputStreamOperator<OrderResult> complexResult = patternStream.select(orderTimeoutOutput,
                (PatternTimeoutFunction<OrderEvent, OrderResult>) (map, l) -> new OrderResult(map.get("begin").get(0).orderId, "timeout"),
                (PatternSelectFunction<OrderEvent, OrderResult>) map -> new OrderResult(map.get("follow").get(0).orderId, "success"));


        DataStream<OrderResult> timeOutResult = complexResult.getSideOutput(orderTimeoutOutput);

        complexResult.print();
        timeOutResult.print();

        env.execute("Order Timeout Detect Job");
    }
}
