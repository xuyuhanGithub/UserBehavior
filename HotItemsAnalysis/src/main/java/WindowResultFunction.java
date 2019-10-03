import entity.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
//        Long itemId = ((Tuple1<Long>) key).f0;
        Long itemId =key;
        Long count=iterable.iterator().next();
        collector.collect(new ItemViewCount(itemId,timeWindow.getEnd(),count));

    }
}
