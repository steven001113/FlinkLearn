package com.sdw.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1. 创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // TODO 2. 读取数据
        DataSource<String> lineDS = env.readTextFile("D:\\java\\javaproject\\FlinkLearn\\input\\word.txt");

        // TODO 3. 切分、转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    collector.collect(wordTuple2);
                }
            }
        });

        // TODO 4. 按照 word 分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndGroupby = wordAndOne.groupBy(0);

        // TODO 5. 各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> result = wordAndGroupby.sum(1);

        // TODO 6. 输出
        result.print();

    }
}
