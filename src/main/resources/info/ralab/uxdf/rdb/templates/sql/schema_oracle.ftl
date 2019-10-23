<#import "column_oracle.ftl" as col>
<#if print_sequence >
##
CREATE SEQUENCE SEQ_NODE INCREMENT BY 1 START WITH 10000;
##
CREATE SEQUENCE SEQ_EVENT INCREMENT BY 1 START WITH 10000;
</#if>

<#list tables as table>
##
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE ${table.name}';
  EXCEPTION WHEN OTHERS THEN NULL;
END;
##
CREATE TABLE ${table.name}
(
<#list table.columns as column>
<@col.colDefine col = column hasNext = column_has_next/><#lt>
</#list>
    constraint ${table.pkName} primary key(<#list table.keys as key>${key}<#if key_has_next>,</#if></#list>)
)
##
<#-- 构建注释 -->
COMMENT ON TABLE ${table.name} IS '${(table.comment)!''}'
<#list table.columns as column>
##
COMMENT ON COLUMN ${table.name}.${column.name} IS '${(column.comment)!''}'
</#list>
<#-- 创建唯一索引-->
<#if table.hasUniqueIndex()>
##
CREATE UNIQUE INDEX ${table.indexName} ON ${table.name} (<#list table.uniqueIndex as index>${index.name} ASC<#if index_has_next>,</#if></#list>)
</#if>

<#-- 创建索引-->
<#if table.hasIndex()>
<#list table.getIndex() as column>
##
CREATE INDEX ${column.indexName} ON ${table.name} (${column.name})
</#list>
</#if>

<#if table_has_next>
</#if>
</#list>