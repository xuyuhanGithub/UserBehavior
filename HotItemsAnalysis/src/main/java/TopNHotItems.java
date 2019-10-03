


import entity.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    private  ListState<ItemViewCount> itemState;

    private final int topSize;
    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<ItemViewCount> descriptor =
                new ListStateDescriptor<ItemViewCount>("itemState-state",ItemViewCount.class);
        itemState = getRuntimeContext().getListState(descriptor);
    }



    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
        itemState.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.windowEnd+1);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<ItemViewCount> allItems = new ArrayList<>();
        for (ItemViewCount item:itemState.get()) {
            allItems.add(item);
        }

        itemState.clear();
        Collections.sort(allItems, new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o2.count-o1.count);
            }
        });

        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
        for (int i=0;i<topSize;i++) {
            ItemViewCount currentItem = allItems.get(i);
            // No1:  商品ID=12224  浏览量=2413
            result.append("No").append(i).append(":")
                    .append("  商品ID=").append(currentItem.itemId)
                    .append("  浏览量=").append(currentItem.count)
                    .append("\n");
        }
        result.append("====================================\n\n");
        Thread.sleep(1000);

        out.collect(result.toString());

    }
}
