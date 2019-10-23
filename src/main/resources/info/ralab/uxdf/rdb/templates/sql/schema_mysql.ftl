<#import "column_mysql.ftl" as col>

<#if print_sequence >
DROP TABLE IF EXISTS sequence;
##
CREATE TABLE sequence (
    name VARCHAR(50) NOT NULL,
    current_value INT NOT NULL,
    increment INT NOT NULL DEFAULT 1,
    PRIMARY KEY (name)
) ENGINE=InnoDB;
##
INSERT INTO sequence VALUES ('SEQ_NODE', 10000, 1);
##
INSERT INTO sequence VALUES ('SEQ_EVENT', 10000, 1);
##
DROP FUNCTION IF EXISTS currval;
##
CREATE FUNCTION currval (seq_name VARCHAR(50))
    RETURNS INTEGER
    LANGUAGE SQL
    DETERMINISTIC
    CONTAINS SQL
    SQL SECURITY DEFINER
    COMMENT ''
BEGIN
    DECLARE value INTEGER;
    SET value = 0;
    SELECT current_value INTO value
    FROM sequence
    WHERE name = seq_name;
    RETURN value;
END
##
DROP FUNCTION IF EXISTS nextval;
##
DELIMITER $
CREATE FUNCTION nextval (seq_name VARCHAR(50))
    RETURNS INTEGER
    LANGUAGE SQL
    DETERMINISTIC
    CONTAINS SQL
    SQL SECURITY DEFINER
    COMMENT ''
BEGIN
    UPDATE sequence
    SET current_value = current_value + increment
    WHERE name = seq_name;
    RETURN currval(seq_name);
END
</#if>


<#list tables as table>
##
DROP TABLE IF EXISTS ${table.name};
##
CREATE TABLE ${table.name}
(
    <#list table.columns as column>
        <@col.colDefine col = column hasNext = column_has_next/><#lt>
    </#list>
    PRIMARY KEY (<#list table.keys as key>`${key}`<#if key_has_next>,</#if></#list>)
) ENGINE=InnoDB COMMENT = '${(table.comment)!''}';
<#-- 创建唯一索引-->
<#if table.hasUniqueIndex()>
##
CREATE UNIQUE INDEX ${table.indexName} ON ${table.name} (<#list table.uniqueIndex as index>${index.name} ASC<#if index_has_next>,</#if></#list>);
</#if>

<#-- 创建索引-->
<#if table.hasIndex()>
<#list table.getIndex() as column>
##
CREATE INDEX ${column.indexName} ON ${table.name} (${column.name})
</#list>
</#if>

</#list>