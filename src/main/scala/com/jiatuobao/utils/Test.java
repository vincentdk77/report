package com.jiatuobao.utils;

import java.util.stream.Stream;

public class Test {
    public static void main(String[] args) {
        Stream.iterate(0, i -> i+1).limit(10).forEach(item ->System.out.println(item));
    }
}
