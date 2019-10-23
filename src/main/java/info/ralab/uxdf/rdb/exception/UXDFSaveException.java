package info.ralab.uxdf.rdb.exception;

import info.ralab.uxdf.UXDFException;
import info.ralab.uxdf.definition.SdOperateType;
import lombok.Getter;

/**
 * UXDF数据保存异常
 */
public class UXDFSaveException extends UXDFException {
    /**
     * 引起保存异常的操作
     */
    @Getter
    private SdOperateType operate;
    /**
     * 错误类型
     */
    @Getter
    private UXDFSaveErrorType errorType;


    public UXDFSaveException() {
        super();
    }

    public UXDFSaveException(final String message, final SdOperateType operate, final UXDFSaveErrorType errorType) {
        super(message);
        this.operate = operate;
        this.errorType = errorType;
    }
}
