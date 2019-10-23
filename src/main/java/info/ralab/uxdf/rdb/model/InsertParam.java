package info.ralab.uxdf.rdb.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class InsertParam {
    private String id;
    private String table;
    private String seqId;
    private String sd;

    private String left;
    private String leftSd;

    private String right;
    private String rightSd;

    private List<String> columns = new ArrayList<>();
    private List<Object> values = new ArrayList<>();
}
