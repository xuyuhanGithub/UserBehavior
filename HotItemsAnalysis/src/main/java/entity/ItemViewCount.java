package entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ItemViewCount {
    public Long itemId;
    public Long windowEnd;
    public Long count;
}
