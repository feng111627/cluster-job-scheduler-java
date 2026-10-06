package com.github.feng111627.clusterjobschedulerjava.tests.scheduler;

import org.junit.jupiter.api.DynamicTest;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * 测试用例收集器：让每条业务用例拥有可读编号与名称，
 * 执行前后在控制台打印分隔线、用例名和结果，方便从 mvn 日志里核对覆盖了哪些业务。
 */
public final class BusinessCaseReporter {

    private final String business;
    private final List<NamedCase> cases = new ArrayList<>();

    public BusinessCaseReporter(String business) {
        this.business = business;
    }

    public void add(String caseId, String name, ThrowingRunnable assertion) {
        cases.add(new NamedCase(caseId, name, assertion));
    }

    public List<DynamicTest> tests() {
        List<DynamicTest> tests = new ArrayList<>();
        tests.add(dynamicTest("00. 业务场景：" + business, () -> {
            System.out.println();
            System.out.println("============================================================");
            System.out.println("业务场景：" + business + "（共 " + cases.size() + " 条用例）");
            System.out.println("============================================================");
            assertTrue(cases.size() > 0, "至少要有一条用例");
        }));
        for (NamedCase c : cases) {
            tests.add(dynamicTest(c.caseId + " " + c.name, () -> {
                System.out.println("[用例 " + c.caseId + "] " + c.name);
                try {
                    c.assertion.run();
                    System.out.println("  -> 结果：PASS");
                } catch (Throwable e) {
                    System.out.println("  -> 结果：FAIL：" + e.getMessage());
                    throw e;
                }
            }));
        }
        return tests;
    }

    /** 断言辅助：延迟求值，失败信息更清楚。 */
    public static void check(boolean condition, Supplier<String> message) {
        assertTrue(condition, message);
    }

    /** 允许用例体内抛出任意异常（如 InterruptedException）。 */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    private record NamedCase(String caseId, String name, ThrowingRunnable assertion) {
    }
}
