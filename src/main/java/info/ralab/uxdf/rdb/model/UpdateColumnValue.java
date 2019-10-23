package info.ralab.uxdf.rdb.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UpdateColumnValue {
    private String column;
    private Object value;
}
