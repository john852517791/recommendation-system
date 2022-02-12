package com.wzy.kafkaStream;/*
 *@Auther: wzy
 *@Date:2021/5/9 -05-09
 *@Descreption: com.wzy.kafkaStream
 *@Version: 1.0
 */

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;


    //    todo：初始化
    @Override
    public void init(ProcessorContext processorContext) {
        this.context=processorContext;

    }

    //    todo：核心处理流程
    @Override
    public void process(byte[] five, byte[] line) {
        String input = new String(line);

//        提取数据，以固定前缀来过滤日志信息
        if (input.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("product rating data coming!>>>"+ input);
            input = input
                    .split("PRODUCT_RATING_PREFIX:")[1]
//                    只取input中的后面的竖线分割的内容
                    .trim();
//            用trim删去前后空格
            context.forward(
                    "logPrecessor".getBytes(),
                    input.getBytes()
            );


        }

    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
