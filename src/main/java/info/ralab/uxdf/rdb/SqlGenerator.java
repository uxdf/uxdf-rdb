package info.ralab.uxdf.rdb;

import info.ralab.uxdf.Sd;
import info.ralab.uxdf.rdb.model.RelationShipRdb;

/**
 * 根据UXDF定义生成SQL
 */
public interface SqlGenerator {

    String TEMPLATE_DIR = "sql/";
    String TEMPLATE_PREFIX = "schema_";
    String TEMPLATE_SUFFIX = ".ftl";
    String TABLE_KEY = "tables";
    String PRINT_SEQUENCE = "print_sequence";

    /**
     * 根据SD定义生成程序用DDL语句
     *
     * @param sd
     * @param sdNames
     * @return
     */
    String generateDDL(final Sd sd, String... sdNames);

    /**
     * @param sd
     * @return
     */
    RelationShipRdb generateRelationShip(final Sd sd);
}
