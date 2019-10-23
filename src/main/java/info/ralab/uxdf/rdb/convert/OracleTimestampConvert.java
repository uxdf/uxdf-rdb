package info.ralab.uxdf.rdb.convert;

import info.ralab.uxdf.UXDFException;
import info.ralab.uxdf.utils.UXDFValueConvert;
import oracle.sql.TIMESTAMP;

import java.sql.SQLException;
import java.util.Date;

/**
 * Oracle 时间戳类型转换
 */
public class OracleTimestampConvert implements UXDFValueConvert<Date> {
    @Override
    public Date convert(Object value) {
        if (value instanceof TIMESTAMP) {
            try {
                return new Date(((TIMESTAMP) value).timestampValue().getTime());
            } catch (SQLException e) {
                throw new UXDFException(e);
            }
        }
        return null;
    }
}
