/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.metrics

import java.lang.management.{BufferPoolMXBean, ManagementFactory}
import javax.management.ObjectName

import scala.collection.mutable
import org.apache.spark.memory.MemoryManager

private[spark] sealed trait MetricGetter {
  def getMetricValue(memoryManager: MemoryManager): Long
  val name = getClass().getName().stripSuffix("$").split("""\.""").last
}

private[spark] abstract class MemoryManagerMetricGetter(
    f: MemoryManager => Long) extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    f(memoryManager)
  }
}

private[spark]abstract class MBeanMetricGetter(mBeanName: String)
  extends MetricGetter {
  val bean = ManagementFactory.newPlatformMXBeanProxy(ManagementFactory.getPlatformMBeanServer,
    new ObjectName(mBeanName).toString, classOf[BufferPoolMXBean])

  override def getMetricValue(memoryManager: MemoryManager): Long = {
    bean.getMemoryUsed
  }
}

private[spark] case object JVMHeapMemory extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getHeapMemoryUsage().getUsed()
  }
}

private[spark] case object JVMOffHeapMemory extends MetricGetter {
  override def getMetricValue(memoryManager: MemoryManager): Long = {
    ManagementFactory.getMemoryMXBean.getNonHeapMemoryUsage().getUsed()
  }
}

private[spark] case object OnHeapExecutionMemory extends MemoryManagerMetricGetter(
  _.onHeapExecutionMemoryUsed)

private[spark] case object OffHeapExecutionMemory extends MemoryManagerMetricGetter(
  _.offHeapExecutionMemoryUsed)

private[spark] case object OnHeapStorageMemory extends MemoryManagerMetricGetter(
  _.onHeapStorageMemoryUsed)

private[spark] case object OffHeapStorageMemory extends MemoryManagerMetricGetter(
  _.offHeapStorageMemoryUsed)

private[spark] case object OnHeapUnifiedMemory extends MemoryManagerMetricGetter(
  (m => m.onHeapExecutionMemoryUsed + m.onHeapStorageMemoryUsed))

private[spark] case object OffHeapUnifiedMemory extends MemoryManagerMetricGetter(
  (m => m.offHeapExecutionMemoryUsed + m.offHeapStorageMemoryUsed))

private[spark] case object DirectPoolMemory extends MBeanMetricGetter(
  "java.nio:type=BufferPool,name=direct")
private[spark] case object MappedPoolMemory extends MBeanMetricGetter(
  "java.nio:type=BufferPool,name=mapped")

private[spark] object MetricGetter {
  val values = IndexedSeq(
    JVMHeapMemory,
    JVMOffHeapMemory,
    OnHeapExecutionMemory,
    OffHeapExecutionMemory,
    OnHeapStorageMemory,
    OffHeapStorageMemory,
    OnHeapUnifiedMemory,
    OffHeapUnifiedMemory,
    DirectPoolMemory,
    MappedPoolMemory
  )

  val cpuUsages = new mutable.HashMap[String, Array[Float]]

  val idxAndValues = values.zipWithIndex.map(_.swap)

  def InitExecutorCpuUsage(executorId: String): Unit = {
    val arr = new Array[Float](5)

    for (i <- 0 to (arr.length - 1)) {
      arr(i) = 100
    }

    cpuUsages(executorId) = arr
  }

  def UpdateExecutorCpuUsage(executorId: String, usage: Float): Unit = {
    if (!cpuUsages.contains(executorId)) {
      InitExecutorCpuUsage(executorId)
    }

    val len = cpuUsages(executorId).length

    for (i <- 0 to (len - 2)) {
      cpuUsages(executorId)(i) = cpuUsages(executorId)(i+1)
    }

    cpuUsages(executorId)(len - 1) = usage
  }

  def ClearExecutorCpuUsage(executorId: String): Unit = {
    cpuUsages -= executorId
  }

  def GetExecutorCpuUsage(executorId: String): Array[Float] = {
    return cpuUsages(executorId)
  }

  def GetExecutorAvgCpuUsage(executorId: String): Float = {
    var avgCpuUsage = 0.0f
    
    for (i <- 0 to (cpuUsages(executorId).length - 1)) {
      avgCpuUsage += cpuUsages(executorId)(i)
    }

    avgCpuUsage /= cpuUsages(executorId).length

    return avgCpuUsage
  }
}
