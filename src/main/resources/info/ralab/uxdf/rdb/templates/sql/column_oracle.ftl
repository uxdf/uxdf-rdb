<#macro colDefine col hasNext>
    <#switch col.type>
    <#case "String">
    ${col.name} NVARCHAR2(#{col.length})<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "Integer">
    ${col.name} INT<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "Datetime">
    ${col.name} TIMESTAMP<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "DateTime">
    ${col.name} TIMESTAMP<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "Boolean">
    ${col.name} INT<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "Float">
    ${col.name} NUMBER<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "Binary">
    ${col.name} NVARCHAR2(100)<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "Clob">
    ${col.name} CLOB<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    <#case "Blob">
    ${col.name} BLOB<#if !col.isNullable> NOT NULL</#if>,
    <#break>
    </#switch> 
</#macro>
