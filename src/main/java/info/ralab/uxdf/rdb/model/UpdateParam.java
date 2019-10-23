package info.ralab.uxdf.rdb.model;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@RequiredArgsConstructor
public class UpdateParam {

    @NonNull
    private String id;
    @NonNull
    private String table;

    private SyncLock syncLock;

    private List<UpdateColumnValue> columnValues = new ArrayList<>();
}
