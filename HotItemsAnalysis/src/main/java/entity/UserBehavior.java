package entity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehavior {
    public Long userId;
    public Long itemId;
    public int category ;
    public String behavior;
    public long timestamp;
}


