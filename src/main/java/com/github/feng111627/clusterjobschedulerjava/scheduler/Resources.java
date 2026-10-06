package com.github.feng111627.clusterjobschedulerjava.scheduler;

/**
 * 资源量（cpu / memoryMb / diskMb），不可变值对象。
 *
 * <p>所有运算都返回新对象，不修改自身，便于在锁内外安全传递快照。
 */
public final class Resources {

    public static final Resources ZERO = new Resources(0, 0, 0);

    private final int cpu;
    private final int memoryMb;
    private final int diskMb;

    public Resources(int cpu, int memoryMb, int diskMb) {
        if (cpu < 0 || memoryMb < 0 || diskMb < 0) {
            throw new IllegalArgumentException("resource amount must be non-negative");
        }
        this.cpu = cpu;
        this.memoryMb = memoryMb;
        this.diskMb = diskMb;
    }

    public int getCpu() {
        return cpu;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public int getDiskMb() {
        return diskMb;
    }

    /** 是否每一维都不小于 other（other 可以被本对象容纳）。 */
    public boolean canFit(Resources other) {
        return cpu >= other.cpu && memoryMb >= other.memoryMb && diskMb >= other.diskMb;
    }

    /** 逐维相加。 */
    public Resources plus(Resources other) {
        return new Resources(cpu + other.cpu, memoryMb + other.memoryMb, diskMb + other.diskMb);
    }

    /** 逐维相减；结果出现负数会抛异常，保证资源账目不会变成负数。 */
    public Resources minus(Resources other) {
        if (!canFit(other)) {
            throw new IllegalArgumentException("insufficient resources: " + this + " - " + other);
        }
        return new Resources(cpu - other.cpu, memoryMb - other.memoryMb, diskMb - other.diskMb);
    }

    /** 逐维取最大值，用于计算集群单节点容量上限。 */
    public Resources max(Resources other) {
        return new Resources(
                Math.max(cpu, other.cpu),
                Math.max(memoryMb, other.memoryMb),
                Math.max(diskMb, other.diskMb));
    }

    /** 三维求和，便于日志与断言展示总资源量。 */
    public int totalUnits() {
        return cpu + memoryMb + diskMb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Resources other)) {
            return false;
        }
        return cpu == other.cpu && memoryMb == other.memoryMb && diskMb == other.diskMb;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(cpu) * 31 * 31 + Integer.hashCode(memoryMb) * 31 + Integer.hashCode(diskMb);
    }

    @Override
    public String toString() {
        return "(cpu=" + cpu + ", mem=" + memoryMb + ", disk=" + diskMb + ")";
    }
}
