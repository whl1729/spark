/* JVMCPUUsage
 *
 * Calculate JVM CPU usage on older than JDK version 7 using MXBeans.
 *
 * First initiate MBeanServerConnection using `openMBeanServerConnection` method. Then
 * create proxy connections to MXBeans using `getMXBeanProxyConnections` and after that
 * poll `getJvmCpuUsage` method periodically.
 *
 * JVMCPUUsage is a free Java class:
 * you can redistribute it and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of the License, or(at your option)
 * any later version. This code is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * See COPYING for a copy of the GNU General Public License. If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2017  Lahiru Pathirage <lpsandaruwan@gmail.com> on 3/27/17.
 */

package org.apache.spark.executor;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class JVMCPUUsage {

    private com.sun.management.OperatingSystemMXBean peOperatingSystemMXBean = (com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
    private RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    // keeping previous timestamps
    private long prevJvmProcessCpuTime = 0;
    private long prevJvmUptime = 0;
    private long curJvmProcessCpuTime = 0;
    private long curJvmUptime = 0;

    // Get JVM CPU usage
    public float getJvmCpuUsage() {
        curJvmProcessCpuTime = peOperatingSystemMXBean.getProcessCpuTime();
        curJvmUptime = runtimeMXBean.getUptime();

        // elapsed process time is in nanoseconds
        long elapsedProcessCpuTime = curJvmProcessCpuTime - prevJvmProcessCpuTime;
        // elapsed uptime is in milliseconds
        long elapsedJvmUptime = curJvmUptime - prevJvmUptime;

        long processorNum = peOperatingSystemMXBean.getAvailableProcessors();

        // total jvm uptime on all the available processors
        long totalElapsedJvmUptime = elapsedJvmUptime * processorNum;

        // calculate cpu usage as a percentage value
        // to convert nanoseconds to milliseconds divide it by 1000000 and to get a percentage multiply it by 100
        float cpuUsage = elapsedProcessCpuTime / (totalElapsedJvmUptime * 10000F);

        System.out.printf("[along]getJvmCpuUsage: \n");
        System.out.printf("[along]prevCpuTime=%dns, curCpuTime=%dns, elapsedCpuTime=%dns.\n",
            prevJvmProcessCpuTime, curJvmProcessCpuTime, elapsedProcessCpuTime);
        System.out.printf("[along]prevUpTime=%dms, curUpTime=%dms, elapsedUpTime=%dms.\n",
            prevJvmUptime, curJvmUptime, elapsedJvmUptime);
        System.out.printf("[along]processorNum=%d, cpuUsage=%f.\n", processorNum, cpuUsage);

        prevJvmProcessCpuTime = curJvmProcessCpuTime;
        prevJvmUptime = curJvmUptime;

        return cpuUsage;
    }
}
