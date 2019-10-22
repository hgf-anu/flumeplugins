package com.yaxin;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    //1. 需要从配置文件中读取的字段.(configure方法中)
    //1.1 两条数据之间的间隔
    private long delay;
    //1.2 body内容
    private String field;

    public void configure(Context context) {
        //构造方法第二个参数是默认值
        delay = context.getLong("delay", 2000L);
        field = context.getString("field", "yaxin");
    }

    /**
     * 处理数据,处理异常try/catch
     */
    public Status process() throws EventDeliveryException {
        //2.处理的单位是event事件,包含两个部分header和body
        try {
            //2.1 header的数据结构是Map
            HashMap<String, String> header = new HashMap<>();
            //2.2 event
            SimpleEvent event = new SimpleEvent();
            //2.3 拿到数据
            for (int i = 0; i < 5; i++) {
                event.setHeaders(header);
                //Mock data
                event.setBody((field + i).getBytes());
                //交给channel处理,频道.getChannelProcessor方法是父类AbstractSource的方法
                getChannelProcessor().processEvent(event);
                //休息delay的时间
                Thread.sleep(delay);
            }
            //2.4 把数据存到event

        } catch (Exception e) {
            //出现异常就回滚
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    /**
     * BackOff回滚,SleepIncrement增加睡眠
     * 间隔时间增长/变大
     */
    public long getBackOffSleepIncrement() {
        return 0;
    }

    /**
     * 最大失败回滚间隔
     */
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
