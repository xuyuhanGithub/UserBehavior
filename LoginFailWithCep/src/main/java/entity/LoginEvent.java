package entity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LoginEvent {
    public Long userId;
    public String ip;
    public String eventType;
    public Long eventTime;
}
