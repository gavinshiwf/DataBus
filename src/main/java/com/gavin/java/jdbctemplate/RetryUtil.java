package com.gavin.java.jdbctemplate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * author:gavin
 * time:2020-11-25
 * jdbc 重试次数
 */
public class RetryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);
    private static final long MAX_SLEEP_MILLSECOND = 256 * 1000;

    /**
     * 重试次数工具方法
     * @param callable 实际逻辑
     * @param retryTimes 最大重试次数(>1)
     * @param sleepTimeInMillisend 运行失败后休眠对应时间再重试
     * @param exponential 休眠时间是否指数递增
     * @param <T> 返回值类型
     * @return 经过重试的callable的执行结果
     */
    public static<T> T executeWithRetry(Callable<T> callable,
                                        int retryTimes,
                                        long sleepTimeInMillisend,
                                        boolean exponential,
                                        List<Class<?>> retryExceptionClass) throws Exception{
        Retry retry = new Retry();
        return retry.deRetry(callable,retryTimes,sleepTimeInMillisend,exponential,retryExceptionClass);

    }

    public static<T> T asyncExecuteWithRetry(Callable<T> callable,
                                        int retryTimes,
                                        long sleepTimeInMillisend,
                                        boolean exponential,
                                        long timeoutMs,
                                        ThreadPoolExecutor executor) throws Exception{
        Retry retry = new AsyncRetry(timeoutMs,executor);
        return retry.deRetry(callable,retryTimes,sleepTimeInMillisend,exponential,null);

    }

    public static ThreadPoolExecutor createThreadPoolExecutor(){
        return new ThreadPoolExecutor(0,5,60L,TimeUnit.SECONDS,new SynchronousQueue<Runnable>());
    }

    private static class Retry{
        public <T> T deRetry(Callable<T> callable,
                             int retryTimes,
                             long sleepTimeInMilliSecond,
                             boolean exponential,
                             List<Class<?>> retryExceptionClass
                             ) throws Exception{
            if(null == callable){
                throw  new IllegalArgumentException("系统错误,参数callable不能为空");
            }
            if(retryTimes < 1){
                throw  new IllegalArgumentException("系统错误,参数retryTime不能小于1");
            }
            Exception saveException = null;
            for(int i = 0;i< retryTimes ;i++){
                try{
                    return call(callable);
                }catch(Exception e){
                    saveException = e;
                    if( i==0 ){
                        LOG.error(String.format("Exception when calling callable,异常Msg:%s",saveException.getMessage()),saveException);
                    }

                    if(null != retryExceptionClass && !retryExceptionClass.isEmpty()){
                        boolean needRetry = false;
                        for(Class<?> eachExceptionClass : retryExceptionClass){
                            if(eachExceptionClass == e.getClass()){
                                needRetry = true;
                                break;
                            }
                        }
                        if(!needRetry){
                            throw saveException;
                        }
                    }

                    for((i+1) < retryTimes && sleepTimeInMilliSecond >0 ) {
                        long startTime = System.currentTimeMillis();

                        long timeToSleep;
                        if(exponential){
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2,i);
                            if(timeToSleep >= MAX_SLEEP_MILLSECOND){
                                timeToSleep = MAX_SLEEP_MILLSECOND;
                            }
                        }else{
                            timeToSleep = sleepTimeInMilliSecond;
                            if(timeToSleep >= MAX_SLEEP_MILLSECOND){
                                timeToSleep = MAX_SLEEP_MILLSECOND;
                            }
                        }

                        try{
                            Thread.sleep(timeToSleep);
                        }catch(InterruptedException ignored){}

                        long realTimeSleep = System.currentTimeMillis()-startTime;
                        LOG.error(String.format("Exception when calling callable,即将尝试执行第%s此重试，本次重试计划等待[%s]ms,实际等待[%s]ms,异常Msg:[%s]",
                                i+1,timeToSleep,realTimeSleep,e.getMessage()));
                    }
                }
            }
            throw saveException;
        }
        protected  <T> T call(Callable<T> callable) throws Exception{
            return callable.call();
        }
    }


    private static class AsyncRetry extends Retry{
        private long timeoutMs;
        private ThreadPoolExecutor executor;

        public AsyncRetry(long timeoutMs,
                          ThreadPoolExecutor executor){
            this.timeoutMs = timeoutMs;
            this.executor = executor;
        }

        /**
         * 使用传入的线程池异步执行任务，并且等待
         * future.get()方法，等待指定的毫秒数，如果任务在超时时间内结束，则正常返回。
         * 如果抛异常(可能是执行超时、执行异常、被其他线程cancel或interrupt)，都记录日志并且网上抛异常。
         * 正常和非正常的情况都会判断任务是否结束，如果没有结束，则cancel任务。cancel参数为true，表示即使任务正则执行，也会interrupt线程
         * @param callable
         * @param <T>
         * @return
         * @throws Exception
         */
        @Override
        protected <T> T call(Callable<T> callable) throws Exception{
            Future<T> future =executor.submit(callable);
            try{
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            }catch(Exception e){
                LOG.warn("try once failed",e);
                throw e;
            }finally {
                if(!future.isDone()){
                    future.cancel(true);
                    LOG.warn("Try once task not done,canel it,active count: "+ executor.getActiveCount());
                }
            }
        }
    }
}
