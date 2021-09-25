package Simulations

import HelperUtils.CreateLogger
import org.cloudbus.cloudsim.allocationpolicies.{VmAllocationPolicyRoundRobin, VmAllocationPolicySimple}
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.{Cloudlet, CloudletSimple}
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.{Datacenter, DatacenterSimple}
import org.cloudbus.cloudsim.hosts.{Host, HostSimple}
import org.cloudbus.cloudsim.provisioners.{PeProvisionerSimple, ResourceProvisionerSimple}
import org.cloudbus.cloudsim.resources.{Pe, PeSimple}
import org.cloudbus.cloudsim.schedulers.cloudlet.{CloudletSchedulerCompletelyFair, CloudletSchedulerSpaceShared, CloudletSchedulerTimeShared}
import org.cloudbus.cloudsim.schedulers.vm.{VmSchedulerSpaceShared, VmSchedulerTimeShared}
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelStochastic
import org.cloudbus.cloudsim.vms.{Vm, VmSimple}
import org.cloudsimplus.builders.tables.CloudletsTableBuilder

import java.util
import collection.JavaConverters.*
import scala.collection.mutable.ListBuffer

class CloudletSchedulerSpaceSharedExample

object CloudletSchedulerSpaceSharedExample:

  val HOSTS:Int = 1
  val HOST_PES = 4

  val VMS = 1
  val VM_PES = 4

  val CLOUDLETS = 4
  val CLOUDLET_PES = 2
  val CLOUDLET_LENGTH = 10000

  val simulation = new CloudSim()
  val logger = CreateLogger(classOf[BasicCloudSimPlusExample])

  def Start() =
    val datacenter0 = createDatacenter(HOSTS, HOST_PES)

    val broker0 = new DatacenterBrokerSimple(simulation)
    val vmList = createVms(VMS)
    val cloudletList = createCloudlets()

    broker0.setVmDestructionDelay(10.0)
    broker0.submitVmList(vmList)
    broker0.submitCloudletList(cloudletList.asJava)

    simulation.start()

    val finishedCloudlets = broker0.getCloudletFinishedList()
    new CloudletsTableBuilder(finishedCloudlets).build()

  def createDatacenter(numHosts: Int, PES: Int): Datacenter =
    val hostList = new ListBuffer[Host]
    for i <- 1 to numHosts do
      hostList.append(createHost(PES))

    logger.info("Hosts" + hostList)

    new DatacenterSimple(simulation, hostList.asJava, new VmAllocationPolicySimple())

  def createHost(PES: Int): Host =
    val peList = new ListBuffer[PeSimple]
    for i <- 1 to PES do
      peList.append(new PeSimple(1000, new PeProvisionerSimple()) )

    logger.info("PE list" + peList.toList)

    val ram:Long = 2048
    val bw:Long = 10000
    val storage:Long = 1000000
    val ramProvisioner = new ResourceProvisionerSimple()
    val bwProvisioner = new ResourceProvisionerSimple()
    val vmScheduler = new VmSchedulerSpaceShared()

    val host = new HostSimple(ram, bw, storage, peList.toList.asJava)

    host
      .setRamProvisioner(ramProvisioner)
      .setBwProvisioner(bwProvisioner)
      .setVmScheduler(vmScheduler)

    host

  def createVms(numVms: Int): java.util.List[Vm] =
    val vmList = new ListBuffer[Vm]
    val vmsAdded = (0 to numVms-1).map ( i => vmList.append(new VmSimple(i, 1000, VM_PES)
      .setRam(512)
      .setBw(1000)
      .setSize(10000)
      .setCloudletScheduler(new CloudletSchedulerSpaceShared()))
    )

    vmList.asJava

  def createCloudlets(): ListBuffer[Cloudlet] =
    val cloudletList = new ListBuffer[Cloudlet]
    val utilizationModel = new UtilizationModelStochastic()

    val cloudletsAdded = (1 to CLOUDLETS) foreach (c => cloudletList.append(new CloudletSimple(c, CLOUDLET_LENGTH, CLOUDLET_PES)
      .setFileSize(1024)
      .setOutputSize(1024)
      .setUtilizationModel(utilizationModel)))

    cloudletList