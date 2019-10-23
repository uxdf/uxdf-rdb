package info.ralab.uxdf.rdb;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Collection;

/**
 * 数据权限
 */
@Data
@AllArgsConstructor
public class DataAuth {
    private Long userId;

    /**
     * 数据和角色的关联表
     */
    private String dataRoleTable;

    /**
     * 角色表
     */
    private String roleTable;

    /**
     * 当前用户角色集合
     */
    private Collection<String> roles;

    /**
     * 是否管理员
     */
    private boolean isAdmin;

    /**
     * 是否基于角色过滤
     *
     * @return
     */
    public boolean isFilterByRoles() {
        return !this.isAdmin && this.dataRoleTable != null && this.roleTable != null && !this.roles.isEmpty();
    }
}
