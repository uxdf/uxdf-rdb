package info.ralab.uxdf.rdb.model;

import com.google.common.collect.Maps;
import info.ralab.uxdf.definition.SdEventDefinition;
import info.ralab.uxdf.definition.SdNodeDefinition;
import lombok.Data;

import java.util.Map;

@Data
public class RelationShipRdb {
    private Map<String, RelationShipRdbTable> node = Maps.newHashMap();
    private Map<String, Map<String, Map<String, RelationShipRdbTable>>> event = Maps.newHashMap();

    public void putNodeRdbTable(final SdNodeDefinition sdNodeImpl, final RelationShipRdbTable rdbTable) {
        this.node.put(sdNodeImpl.getNodeName(), rdbTable);
    }

    public void putEventRdbTable(final SdEventDefinition sdEventImpl, final RelationShipRdbTable rdbTable) {
        final String eventName = sdEventImpl.getEventName();
        final String leftNodeName = sdEventImpl.getLeftNodeName();
        final String rightNodeName = sdEventImpl.getRightNodeName();
        Map<String, Map<String, RelationShipRdbTable>> leftMap = this.event.computeIfAbsent(eventName, key -> Maps.newHashMap());
        Map<String, RelationShipRdbTable> rightMap = leftMap.computeIfAbsent (leftNodeName, key -> Maps.newHashMap());
        rightMap.put(rightNodeName, rdbTable);
    }
}
