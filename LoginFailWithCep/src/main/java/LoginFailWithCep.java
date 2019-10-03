import entity.LoginEvent;
import entity.LoginEventOut;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class LoginFailWithCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        ArrayList<LoginEvent> data = new ArrayList<>();
        data.add(new LoginEvent(new Long(1), "192.168.0.1", "fail", new Long(1558430842)));
        data.add(new LoginEvent(new Long(1), "192.168.0.2", "fail", new Long(1558430843)));
        data.add(new LoginEvent(new Long(1), "192.168.0.3", "fail", new Long(1558430844)));
        data.add(new LoginEvent(new Long(1), "192.168.10.10", "success", new Long(1558430845)));

        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromCollection(data).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent loginEvent) {
                return loginEvent.eventTime * 1000;
            }
        });


        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("begin").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return value.eventType.equals("fail");
            }
        }).next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return value.eventType.equals("fail");
            }
        }).within(Time.seconds(2));


        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream, loginFailPattern);

        SingleOutputStreamOperator<LoginEventOut> loginFailDataStream = patternStream.select(new PatternSelectFunction<LoginEvent, LoginEventOut>() {
            @Override
            public LoginEventOut select(Map<String, List<LoginEvent>> pattern) throws Exception {
                LoginEvent first = pattern.getOrDefault("begin", null).iterator().next();
                LoginEvent second = pattern.getOrDefault("next", null).iterator().next();

                return new LoginEventOut(second.userId, second.ip + "前一个是: " + first.ip, second.eventType);
            }
        });

        loginFailDataStream.print();
        env.execute("Login Fail Detect Job");

    }
}
