package com.zhan.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 业务测试代码 (一个流处理、一个批处理)
 */
@RequestMapping("test")
@RestController
public class TestController {
 
    @GetMapping("test1")
    public void test(Integer parallelism) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //设置并行度
        env.setParallelism(parallelism);
        
        //读取文本流
        DataStreamSource<String> source = env.socketTextStream("192.168.133.133", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = s.split(" ");
                for (String split : splits) {
                    collector.collect(new Tuple2<>(split, 1));
                }
            }
        }).filter(data -> StringUtils.isNotEmpty(data.f0)).keyBy(data -> data.f0).sum(1);
        //打印
        operator.print();
        //执行
        env.execute();
    }
 
    @GetMapping("test2")
    public void test2() throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 
        //设置并行度
        env.setParallelism(1);
 
        //将一列元素作为数据源
        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
 
        //控制台打印
        integerDataStream.print("int");
 
        //执行任务
        env.execute();
    }
}