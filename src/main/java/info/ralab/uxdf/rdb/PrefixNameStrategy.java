package info.ralab.uxdf.rdb;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.ralab.uxdf.rdb.model.Table;
import info.ralab.uxdf.rdb.utils.NameUtils;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 * <p>基于前缀的sql语句生成命名策略</p>
 * <pre>
 * 通过添加前缀来通过节点名生成数据库相关名称包括：
 * nodePrefix：        表明前缀
 * indexPrefix:     索引名前缀
 * seqPrefix:       序列名前缀
 * attr:            公共属性名前缀
 * prop:            特定属性名前缀
 * </pre>
 * Copyright: Copyright (c) 2017
 * Company:
 * PrefixNameStrategy.java
 *
 * @author pengqin
 * @version 1.0
 */
@Data
@Component
@ConfigurationProperties(prefix = "truedata.rdb.sql.generator.name")
@Validated
public class PrefixNameStrategy implements NameStrategy {

    @NotNull
    private String nodePrefix;
    @NotNull
    private String nodePkPrefix;
    @NotNull
    private String eventPrefix;
    @NotNull
    private String eventPkPrefix;
    @NotNull
    private String indexPrefix;
    @NotNull
    private String seqPrefix;
    @NotNull
    private String trgPrefix;
    @NotNull
    private String attrPrefix;
    @NotNull
    private String propPrefix;
    @NotNull
    private String redundancyPropPrefix;
    @NotNull
    private Integer maxLength;
    @NotNull
    private Map<String, String> repositoryUser = Maps.newHashMap();

    private String addPrefixToName(final String prefix, final String name) {
        return (prefix + name).toUpperCase();
    }

    @Override
    public String nodeToTableName(final String nodeName) {
        return addPrefixToName(nodePrefix, NameUtils.camelToUnderline(nodeName));
    }

    @Override
    public String nodeToTablePkName(String nodeName) {
        return addPrefixToName(nodePkPrefix, NameUtils.camelToUnderline(nodeName));
    }

    @Override
    public String eventToTableName(String eventName, String leftName, String rightName) {
        return addPrefixToName(
                eventPrefix,
                Joiner.on(NameUtils.UNDER_LINE).join(
                        NameUtils.camelToUnderline(leftName),
                        eventName,
                        NameUtils.camelToUnderline(rightName)
                )
        );
    }

    @Override
    public String eventToTablePkName(String eventName, String leftName, String rightName) {
        return addPrefixToName(
                eventPkPrefix,
                Joiner.on(NameUtils.UNDER_LINE).join(
                        NameUtils.camelToUnderline(leftName),
                        eventName,
                        NameUtils.camelToUnderline(rightName)
                )
        );
    }

    @Override
    public String attrToColumnName(String attrName) {
        String name = attrName.startsWith("__") ? attrName.substring(2) : attrName;
        return addPrefixToName(attrPrefix, NameUtils.camelToUnderline(name));
    }

    @Override
    public String propToColumnName(String propName) {
        return addPrefixToName(propPrefix, NameUtils.camelToUnderline(propName));
    }

    @Override
    public String redundancyPropToColumnName(String propName) {
        return addPrefixToName(redundancyPropPrefix, NameUtils.camelToUnderline(propName));
    }

    /**
     * 通过表名创建索引名称
     *
     * @param tableName 表名
     * @return 索引名
     */
    @Override
    public String indexNameOfTable(final String tableName) {
        return addPrefixToName(indexPrefix, tableName);
    }

    @Override
    public String triggerNameOfTable(String tableName, String keyName) {
        return addPrefixToName(trgPrefix, Joiner.on(NameUtils.UNDER_LINE).join(tableName, keyName));
    }

    @Override
    public void checkName(List<Table> tables) {
        //判断表名是否超长或重复
        //TODO 此处代码可以优化，将需要判断超长的名称统一处理
        tables.forEach(table -> {
            String name = table.getName();
            if (name.length() > maxLength) {
                int subNum = 1;
                String newName = transformNameWithLength(name, maxLength, subNum);
                table.setName(newName);
                if (tables.stream().filter(t -> t.getName().equals(table.getName())).count() > 1) {
                    throw new RuntimeException(String.format(
                            "table name [%s|%s] repeat after abbreviation.",
                            name,
                            newName
                    ));
                }

                if (table.getIndexName() != null) {
                    table.setIndexName(this.indexNameOfTable(table.getName()));
                }

            }
            // 判断主键名是否超长
            String pkName = table.getPkName();
            if (pkName != null && pkName.length() > maxLength) {
                int subNum = 1;
                String newPkName = transformNameWithLength(pkName, maxLength, subNum);
                table.setPkName(newPkName);
                if (tables.stream().filter(t -> pkName.equals(t.getPkName())).count() > 1) {
                    throw new RuntimeException(String.format(
                            "pk name [%s|%s] repeat after abbreviation.",
                            pkName,
                            newPkName
                    ));
                }
            }
            //判断索引名是否超长
            String indexName = table.getIndexName();
            if (indexName != null && indexName.length() > maxLength) {
                int subNum = 1;
                String newIndexName = transformNameWithLength(indexName, maxLength, subNum);
                table.setIndexName(newIndexName);
                if (tables.stream().filter(t -> indexName.equals(t.getIndexName())).count() > 1) {
                    throw new RuntimeException(String.format(
                            "index name [%s|%s] repeat after abbreviation.",
                            indexName,
                            newIndexName
                    ));
                }
            }
            //判断序列名是否超长
            String seqName = table.getSeqName();
            if (seqName != null && seqName.length() > maxLength) {
                int subNum = 1;
                String newSeqName = transformNameWithLength(seqName, maxLength, subNum);
                table.setSeqName(newSeqName);
                if (tables.stream().filter(t -> seqName.equals(t.getSeqName())).count() > 1) {
                    throw new RuntimeException(String.format(
                            "sequence name [%s|%s] repeat after abbreviation.",
                            seqName,
                            newSeqName
                    ));
                }
            }
        });
    }

    private String transformNameWithLength(String name, int maxLength, int subNum) {
        Splitter splitter = Splitter.on(NameUtils.UNDER_LINE);
        Joiner joiner = Joiner.on(NameUtils.UNDER_LINE);

        List<String> words = splitter.splitToList(name);
        List<String> newWords = Lists.newArrayListWithCapacity(words.size());
        for (int i = 0, len = words.size(); i < len; i++) {
            String word = words.get(i);
            if (i == 0) {
                if (newWords.size() <= i) {
                    newWords.add(word);
                } else {
                    newWords.set(i, word);
                }
            } else {
                if (newWords.size() <= i) {
                    newWords.add(word.substring(0, subNum));
                } else {
                    newWords.set(i, word.substring(0, subNum));
                }
            }
        }
        String shotName = joiner.join(newWords);
        if (shotName.length() > maxLength) {
            throw new RuntimeException(String.format("[%s] is too long.", shotName));
        }
        return shotName;
    }
}
