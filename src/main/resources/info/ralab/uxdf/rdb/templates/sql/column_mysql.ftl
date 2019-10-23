<#macro colDefine col hasNext>
    <#switch col.type>
    <#case "String">
    `${col.name}` VARCHAR(#{col.length})<#if col.isNullable> NULL<#else> NOT NULL</#if> COMMENT '${(col.comment)!''}',
    <#break>
    <#case "Integer">
    `${col.name}` INT<#if col.isNullable> NULL<#else> NOT NULL</#if> COMMENT '${(col.comment)!''}',
    <#break>
    <#case "Datetime">
    `${col.name}` TIMESTAMP(6)<#if col.isNullable> NULL<#else> NOT NULL</#if> COMMENT '${(col.comment)!''}',
    <#break>
    <#case "Boolean">
    `${col.name}` TINYINT(1)<#if col.isNullable> NULL<#else> NOT NULL DEFAULT 0</#if> COMMENT '${(col.comment)!''}',
    <#break>
    <#case "Float">
    `${col.name}` DOUBLE<#if col.isNullable> NULL<#else> NOT NULL</#if> COMMENT '${(col.comment)!''}',
    <#break>
    <#case "Binary">
    `${col.name}` VARCHAR(100)<#if col.isNullable> NULL<#else> NOT NULL</#if> COMMENT '${(col.comment)!''}',
    <#break>
    <#case "Clob">
    `${col.name}` LONGTEXT<#if col.isNullable> NULL<#else> NOT NULL</#if> COMMENT '${(col.comment)!''}',
    <#break>
    <#case "Blob">
    `${col.name}` LONGBLOB<#if col.isNullable> NULL<#else> NOT NULL</#if> COMMENT '${(col.comment)!''}',
    <#break>
    </#switch>
</#macro>
