package java.com.gavin.java.exception;

/**
 * author:gavin
 * info:自定义EtlException
 * time:2020-11-19
 */
public class EtlException extends RuntimeException {
    public EtlException(String message){
        super(message);
    }

    public EtlException(String message,Throwable cause){
        super(message,cause);
    }

    public EtlException(Throwable cause){
        super(cause);
    }

    protected EtlException(String message,Throwable cause,
                           boolean enableSuppression,
                           boolean writeableStackTrace){
        super(message,cause,enableSuppression,writeableStackTrace);
    }
}
