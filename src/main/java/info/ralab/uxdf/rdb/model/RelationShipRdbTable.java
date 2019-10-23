package info.ralab.uxdf.rdb.model;

import com.google.common.collect.Maps;
import lombok.Data;

import java.util.Map;

@Data
public class RelationShipRdbTable {
    private String name;
    private String seqId;
    private Map<String, String> column = Maps.newHashMap();
}
