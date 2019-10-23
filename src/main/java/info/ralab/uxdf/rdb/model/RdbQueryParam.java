package info.ralab.uxdf.rdb.model;

import info.ralab.uxdf.model.SdDataQueryLogic;
import lombok.*;

@Data
@AllArgsConstructor
public class RdbQueryParam {
    private String column;
    private Object value;
    private SdDataQueryLogic logic = SdDataQueryLogic.EQ;

    public String getExpression() {
        switch (logic) {
            case EQ:
                return "=";
            case EW:
                return "like";
            case GT:
                return ">";
            case LT:
                return "<";
            case NE:
                return "<>";
            case NN:
                return "IS NOT NULL";
            case SW:
                return "like";
            case GTE:
                return ">=";
            case LTE:
                return "<=";
            case LIKE:
                return "like";
            case NULL:
                return "IS NULL";
            default:
                return "=";
        }
    }

    public Object getValue() {
        switch (logic) {
            case EW:
                return "%" + value;
            case SW:
                return value + "%";
            case LIKE:
                return "%" + value + "%";
            default:
                return value;
        }
    }
}
