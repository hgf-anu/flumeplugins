package com.yaxin;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    private String prefix = "";
    private String suffix = "";

    //slf4j框架,获取日志
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        Channel channel = getChannel();

        Transaction transaction = channel.getTransaction();

        //事务控制--开始事务
        transaction.begin();

        try {
            //1.处理数据
            //1.1拿数据,可能拿不到,channel不一定有数据,最好进行判断
            Event take;
            while ((take = channel.take()) == null) {
                Thread.sleep(200);
            }

            //1.2确实拿到数据了,通过logger把数据传出去
            LOG.info(prefix + new String(take.getBody()) + suffix);

            //1.3处理完数据后,提交事务
            transaction.commit();

            status = Status.READY;

        } catch (Throwable t) {
            transaction.rollback();
            status = Status.BACKOFF;

            //有的问题处理不了,是Error类型的就抛出
            if (t instanceof Error) {
                throw (Error) t;
            }
        }finally {
            //1.4记住一定要关闭事务
            transaction.close();
        }

        return status;
    }

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix", "PRE");
        suffix = context.getString("suffix");
    }
}
