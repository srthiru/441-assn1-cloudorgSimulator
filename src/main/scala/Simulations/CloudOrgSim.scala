package Simulations

import org.cloudbus.cloudsim.allocationpolicies.{VmAllocationPolicyAbstract, VmAllocationPolicyBestFit, VmAllocationPolicyFirstFit, VmAllocationPolicyRandom, VmAllocationPolicyRoundRobin, VmAllocationPolicySimple}
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple
import org.cloudbus.cloudsim.cloudlets.{Cloudlet, CloudletSimple}
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.datacenters.{Datacenter, DatacenterSimple}
import org.cloudbus.cloudsim.distributions.{ContinuousDistribution, StatisticalDistribution}
import org.cloudbus.cloudsim.hosts.{Host, HostSimple}
import org.cloudbus.cloudsim.provisioners.{PeProvisionerSimple, ResourceProvisionerSimple}
import org.cloudbus.cloudsim.resources.{Pe, PeSimple}
import org.cloudbus.cloudsim.schedulers.cloudlet.{CloudletSchedulerAbstract, CloudletSchedulerCompletelyFair, CloudletSchedulerSpaceShared, CloudletSchedulerTimeShared}
import org.cloudbus.cloudsim.schedulers.vm.{VmSchedulerAbstract, VmSchedulerSpaceShared, VmSchedulerTimeShared}
import org.cloudbus.cloudsim.utilizationmodels.{UtilizationModel, UtilizationModelDynamic, UtilizationModelFull, UtilizationModelStochastic}
import org.cloudbus.cloudsim.vms.{Vm, VmCost, VmSimple}
import org.cloudsimplus.builders.tables.{CloudletsTableBuilder, Table, TableBuilderAbstract, TableColumn}
import org.cloudsimplus.autoscaling.{HorizontalVmScaling, HorizontalVmScalingSimple}

import com.typesafe.config.{Config, ConfigFactory}

import java.text.NumberFormat
import java.util.Locale
import java.util.function.Supplier

import collection.JavaConverters.*

import HelperUtils.{CreateLogger, CustomCloudletsTable, ObtainConfigReference, VmCreator}


class CloudOrgSim

object CloudOrgSim:

  // Creating logger instance
  val logger = CreateLogger(classOf[CloudOrgSim])
  val cloudsim = new CloudSim(0.05)
  val currencyFormat: NumberFormat = NumberFormat.getCurrencyInstance(Locale.US);

  def runSim(simName: String, configName: String = "application.conf") =

    // Get Config reference for the simulation
    val simConfig = ConfigFactory.load(configName).getConfig("cloudSimulator."+simName)
    logger.info("Loaded config for " + simName)
    
    val broker0 = new DatacenterBrokerSimple(cloudsim)

    val dcConfig = simConfig.getConfig("datacenters")
    val dataCenters = (1 to dcConfig.getInt("nDcs")).map(i => createDatacenter(dcConfig.getConfig("datacenter" + i), cloudsim))

    val cloudletConf = simConfig.getConfig("cloudlet")
    val cloudletList = (1 to simConfig.getInt("nCloudlets")).map( i => createCloudlet(i, cloudletConf))

    val vmConf = simConfig.getConfig("vm")
    val vmCreator = new VmCreator(vmConf)
    val vmList = (1 to simConfig.getInt("nVms")).map (i => vmCreator.createScalableVm())

    broker0.submitVmList(vmList.asJava)
    broker0.submitCloudletList(cloudletList.asJava)

    logger.info("Starting cloud simulation...")
    cloudsim.start()

    val finishedCloudlets = broker0.getCloudletFinishedList()
//    val costs = getCloudletCost(finishedCloudlets)
//    val costs = finishedCloudlets.map(_.getTotalCost)

    val cloudletTable = new CustomCloudletsTable(finishedCloudlets)
    // Todo: Try to add a new column to Cloudlet Table builder directly instead of creating a new class
    // Problem: Cannot access protected method addColumn from the base _Table_ class
    //    val col:TableColumn = cloudletTable.addColumn("StartTime", "").setFormat()
    //    cloudletTable.addColumn(new TableColumn(), )
    //    logger.info("Total cost: ", costs)

    cloudletTable.build()

    val cloudletTotalCost = cloudletList.map(i => i.getTotalCost()).sum
    logger.info("Total Cloudlet Cost: " + currencyFormat.format(cloudletTotalCost))
    logger.info("Avg. Cost per Cloudlet: " + currencyFormat.format(cloudletTotalCost/cloudletList.length))

    showVmCosts(vmList)

    logger.info("Overall time taken for the execution of the cloudlets is: " + cloudsim.clockStr() + " secs")

  def createDatacenter(dcConfig: Config, cloudsim: CloudSim): Datacenter =
    val nHosts = dcConfig.getInt("nHosts")
    logger.info("Data center config: " + dcConfig)
    val hostList = (1 to dcConfig.getInt("nHosts")).map (i => createHosts(dcConfig.getConfig("host")))
    val dc = new DatacenterSimple(cloudsim, hostList.toList.asJava)

    val vmAllocationPolicy = if dcConfig.hasPath("vmAllocationPolicy") then getVmAllocationPolicy(dcConfig.getString("vmAllocationPolicy")) else new VmAllocationPolicySimple()

    dc.setVmAllocationPolicy(vmAllocationPolicy)

    dc.getCharacteristics()
      .setCostPerSecond(dcConfig.getDouble("chars.cpuCost"))
      .setCostPerMem(dcConfig.getDouble("chars.ramCost"))
      .setCostPerStorage(dcConfig.getDouble("chars.storCost"))
      .setCostPerBw(dcConfig.getDouble("chars.bwCost"))

    dc.setSchedulingInterval(60)

    dc

  def createHosts(hostConfig: Config): Host =

    val peMips = hostConfig.getInt("mipsCapacity")/hostConfig.getInt("nPEs")
    val peList = (1 to hostConfig.getInt("nPEs")).map (i => new PeSimple(peMips, new PeProvisionerSimple()))

    val ram:Long = hostConfig.getInt("RAMInMBs")
    val bw:Long = hostConfig.getInt("BandwidthInMBps")
    val storage:Long = hostConfig.getInt("StorageInMBs")
    val ramProvisioner = new ResourceProvisionerSimple()
    val bwProvisioner = new ResourceProvisionerSimple()
    val vmScheduler = if hostConfig.hasPath("vmScheduler") then getVmScheduler(hostConfig.getString("vmScheduler")) else new VmSchedulerSpaceShared()

    val host = new HostSimple(ram, bw, storage, peList.asJava)

    host
      .setRamProvisioner(ramProvisioner)
      .setBwProvisioner(bwProvisioner)
      .setVmScheduler(vmScheduler)

    host

  def createCloudlet(i: Int, cloudletConf: Config): Cloudlet =

    val utilizationModel = if cloudletConf.hasPath("cloudletUtilModel") then getUtilModel(cloudletConf.getString("cloudletUtilModel")) else new UtilizationModelFull()

    new CloudletSimple(i, cloudletConf.getInt("length"), cloudletConf.getInt("PEs"))
      .setSizes(cloudletConf.getInt("sizes"))
      .setUtilizationModel(utilizationModel.asInstanceOf[UtilizationModel])

  def getUtilModel(utilModel: String): UtilizationModel ={
    return if utilModel == "stochastic" then new UtilizationModelStochastic() else if utilModel == "dynamic" then new UtilizationModelDynamic() else UtilizationModelFull()
  }

  def getCloudletScheduler(cloudletScheduler: String): CloudletSchedulerAbstract = {
    return if cloudletScheduler == "time" then new CloudletSchedulerTimeShared() else if cloudletScheduler == "space" then new CloudletSchedulerSpaceShared() else new CloudletSchedulerCompletelyFair()
  }

  def getVmScheduler(vmScheduler: String): VmSchedulerAbstract = {
    return if vmScheduler == "time" then new VmSchedulerTimeShared() else new VmSchedulerSpaceShared()
  }

  def getVmAllocationPolicy(vmAllocationPolicy: String): VmAllocationPolicyAbstract = {
    return (if vmAllocationPolicy == "round" then new VmAllocationPolicyRoundRobin()
            else if vmAllocationPolicy == "random" then new VmAllocationPolicyRandom(createContinuousDistribution())
            else if vmAllocationPolicy == "best" then new VmAllocationPolicyBestFit()
            else if vmAllocationPolicy == "first" then new VmAllocationPolicyFirstFit()
            else new VmAllocationPolicySimple())
  }

  def createContinuousDistribution(): ContinuousDistribution ={
    new ContinuousDistribution {
      override def probability(x: Double): Double = ???

      override def density(x: Double): Double = ???

      override def cumulativeProbability(x: Double): Double = ???

      override def cumulativeProbability(x0: Double, x1: Double): Double = ???

      override def inverseCumulativeProbability(p: Double): Double = ???

      override def getNumericalMean: Double = ???

      override def getNumericalVariance: Double = ???

      override def getSupportLowerBound: Double = ???

      override def getSupportUpperBound: Double = ???

      override def isSupportLowerBoundInclusive: Boolean = ???

      override def isSupportUpperBoundInclusive: Boolean = ???

      override def isSupportConnected: Boolean = ???

      override def reseedRandomGenerator(seed: Long): Unit = ???

      override def sample(sampleSize: Int): Array[Double] = ???

      override def originalSample(): Double = ???

      override def getSeed: Long = ???

      override def isApplyAntitheticVariates: Boolean = ???

      override def setApplyAntitheticVariates(applyAntitheticVariates: Boolean): StatisticalDistribution = ???
    }
  }

  def showVmCosts(vmList: IndexedSeq[Vm]): Unit =
    (0 to vmList.length-1).foreach(i => logger.info("Processing Cost of VM" + i + ": "+ {
      val vmCost = new VmCost(vmList(i))
      currencyFormat.format(vmCost.getProcessingCost())
    }))

    (0 to vmList.length-1).foreach(i => logger.info("Memory Cost of VM" + i + ": "+ {
      val vmCost = new VmCost(vmList(i))
      currencyFormat.format(vmCost.getMemoryCost())
    }))

    (0 to vmList.length-1).foreach(i => logger.info("Bandwidth Cost of VM" + i + ": "+ {
      val vmCost = new VmCost(vmList(i))
      currencyFormat.format(vmCost.getBwCost())
    }))

    (0 to vmList.length-1).foreach(i => logger.info("Bandwidth Cost of Storage" + i + ": "+ {
      val vmCost = new VmCost(vmList(i))
      currencyFormat.format(vmCost.getStorageCost())
    }))

    (0 to vmList.length-1).foreach(i => logger.info("Cost of VM" + i + ": "+ {
      val vmCost = new VmCost(vmList(i))
      currencyFormat.format(vmCost.getTotalCost())
    }))












