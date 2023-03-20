package com.cyang.batch;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ListSplitter implements Iterator<List<Message>> {
    private int sizeLimit = 1000 * 1000;
    private List<Message> messages;
    private int currentIndex;

    public ListSplitter(List<Message> messages){
        this.messages = messages;
    }
    public boolean hasNext() {
        return currentIndex < messages.size();
    }

    public List<Message> next() {
        int nextIndex = currentIndex;
        int totalSize = 0;

        for (; nextIndex < messages.size(); nextIndex++) {
            Message msg = messages.get(nextIndex);
            int tmpSize = msg.getTopic().length() + msg.getBody().length;
            Map<String, String> properties = msg.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            // 增加日志的开销20字节
            tmpSize = tmpSize + 20;
            if (tmpSize > sizeLimit) {
                //单个消息超过了最大的限制（1M），否则会阻塞进程
                if (nextIndex - currentIndex == 0) {
                    //假如下一个子列表没有元素,则添加这个子列表然后退出循环,否则退出循环
                    nextIndex++;
                }
                break;
            }
            if (tmpSize + totalSize > sizeLimit) {
                break;
            } else {
                totalSize += tmpSize;
            }
        }
        List<Message> subList = messages.subList(currentIndex, nextIndex);
        currentIndex = nextIndex;
        return subList;
    }

    public void remove() {

    }
}
