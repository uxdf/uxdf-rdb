package info.ralab.uxdf.rdb.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

/**
 * 同步锁
 */
@Data
@AllArgsConstructor
public class SyncLock {
    @NonNull
    private String column;
    private Object value;
}
