package com.github.feng111627.clusterjobschedulerjava.model;

/**
 * 资源向量（CPU 核数 / 内存 MB）。不可变，所有运算返回新对象，
 * 避免账目对象在多处被原地修改。
 */
public final class Resource {
    public static final Resource ZERO = new Resource(0, 0);

    private final int cpu;
    private final int memoryMb;

    public Resource(int cpu, int memoryMb) {
        if (cpu < 0 || memoryMb < 0) {
            throw new IllegalArgumentException("资源量不能为负数: cpu=" + cpu + ", memoryMb=" + memoryMb);
        }
        this.cpu = cpu;
        this.memoryMb = memoryMb;
    }

    public int getCpu() {
        return cpu;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    /** 逐维相加。 */
    public Resource add(Resource other) {
        return new Resource(this.cpu + other.cpu, this.memoryMb + other.memoryMb);
    }

    /**
     * 逐维相减。不允许减成负数——这是“可用资源不能算成负数、
     * 重复释放不能越算越多”这条账目的底层防线。
     */
    public Resource subtract(Resource other) {
        int newCpu = this.cpu - other.cpu;
        int newMem = this.memoryMb - other.memoryMb;
        if (newCpu < 0 || newMem < 0) {
            throw new IllegalStateException(
                    "资源账目不能为负: 当前=" + this + ", 试图扣减=" + other);
        }
        return new Resource(newCpu, newMem);
    }

    /** this 是否在每一维度上都能容纳 other（other &lt;= this）。 */
    public boolean canFit(Resource other) {
        return other.cpu <= this.cpu && other.memoryMb <= this.memoryMb;
    }

    /** 是否为零向量。 */
    public boolean isZero() {
        return cpu == 0 && memoryMb == 0;
    }

    @Override
    public String toString() {
        return "(cpu=" + cpu + ", memoryMb=" + memoryMb + ")";
    }
}
