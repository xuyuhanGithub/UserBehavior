import entity.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg  implements AggregateFunction<UserBehavior,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long acc) {
        return acc+1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}
