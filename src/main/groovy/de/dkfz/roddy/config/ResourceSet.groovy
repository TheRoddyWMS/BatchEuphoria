package de.dkfz.roddy.config

import de.dkfz.roddy.tools.BufferValue
import de.dkfz.roddy.tools.RoddyConversionHelperMethods
import de.dkfz.roddy.tools.TimeUnit
import groovy.transform.CompileStatic

import java.time.Duration

@CompileStatic
class ResourceSet {
    private final String queue
    private ResourceSetSize size
    /**
     * The target memory value.
     */
    private BufferValue mem
    private Integer cores
    private Integer nodes
    private TimeUnit walltime
    private Integer nthreads; //Number of currently active threads of a job
    private BufferValue swap; //Total virtual maxMemory (swap) usage of all processes in a job
    /**
     * Hard disk storage used.
     */
    private BufferValue storage
    private String additionalNodeFlag

    ResourceSet(BufferValue mem, Integer cores, Integer nodes, Duration walltime, BufferValue storage, String queue, String additionalNodeFlag) {
        this.mem = mem
        this.cores = cores
        this.nodes = nodes
        this.walltime = walltime ? new TimeUnit(walltime.getSeconds()+"s") : null
        this.storage = storage
        this.queue = queue
        this.additionalNodeFlag = additionalNodeFlag
    }

    ResourceSet(ResourceSetSize size, BufferValue mem, Integer cores, Integer nodes, TimeUnit walltime, BufferValue storage, String queue, String additionalNodeFlag) {
        this.size = size
        this.mem = mem
        this.cores = cores
        this.nodes = nodes
        this.walltime = walltime
        this.storage = storage
        this.queue = queue
        this.additionalNodeFlag = additionalNodeFlag
    }

    ResourceSetSize getSize() {
        return size
    }

    ResourceSet clone() {
        return new ResourceSet(size, mem, cores, nodes, walltime, storage, queue, additionalNodeFlag)
    }

    BufferValue getMem() {
        return mem
    }

    Integer getCores() {
        return cores
    }

    Integer getNodes() {
        return nodes
    }

    BufferValue getStorage() {
        return storage
    }

    boolean isMemSet() {
        return mem != null
    }

    boolean isCoresSet() {
        return cores != null
    }

    boolean isNodesSet() {
        return nodes != null
    }

    boolean isStorageSet() {
        return storage != null
    }

    TimeUnit getWalltime() {
        return walltime
    }

    Duration getWalltimeAsDuration() {
        String[] wt = walltime.toString().split(":")
        return Duration.ofDays(Long.parseLong(wt[0])).plusHours(Long.parseLong(wt[1])).plusMinutes(Long.parseLong(wt[2])).plusSeconds(Long.parseLong(wt[3]))
    }

    boolean isWalltimeSet() {
        return walltime != null
    }

    boolean isQueueSet() {
        return !RoddyConversionHelperMethods.isNullOrEmpty(queue)
    }

    String getQueue() {
        return queue
    }

    boolean isAdditionalNodeFlagSet() {
        return !RoddyConversionHelperMethods.isNullOrEmpty(additionalNodeFlag)
    }

    String getAdditionalNodeFlag() {
        return additionalNodeFlag
    }

    Integer getNumberOfThreads(){
        return nthreads
    }

    BufferValue getSwap(){
        return swap
    }
}
