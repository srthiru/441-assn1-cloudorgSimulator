package Simulations

import org.cloudbus.cloudsim.allocationpolicies.{VmAllocationPolicyAbstract, VmAllocationPolicyBestFit, VmAllocationPolicyFirstFit, VmAllocationPolicyRandom, VmAllocationPolicyRoundRobin, VmAllocationPolicySimple}
import org.cloudbus.cloudsim.brokers.{DatacenterBrokerAbstract, DatacenterBrokerBestFit, DatacenterBrokerFirstFit, DatacenterBrokerSimple}
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
import org.cloudsimplus.builders.tables.{CloudletsTableBuilder, Table, TableBuilderAbstract, TableColumn, TextTableColumn}
import org.cloudsimplus.autoscaling.{HorizontalVmScaling, HorizontalVmScalingSimple}
import com.typesafe.config.{Config, ConfigFactory}
import org.cloudsimplus.util.Log

import java.text.NumberFormat
import java.util.Locale
import collection.JavaConverters.*
import HelperUtils.{CreateLogger, ObtainConfigReference, VmCreator}
import ch.qos.logback.classic.Level



class CloudOrgSim

object CloudOrgSim:

  // Creating logger instance
  val logger = CreateLogger(classOf[CloudOrgSim])
  val currencyFormat: NumberFormat = NumberFormat.getCurrencyInstance(Locale.US);

  /**
   * Runs a simulation for a given configuration specified thourgh the simulation name and config file name
   *
   * @param simName name of the simulation to be run
   * @param configName name of the config file to be used
   */
  def runSim(simName: String, configName: String = "application.conf") =

    val cloudsim = new CloudSim(0.05)

    Log.setLevel(Level.INFO)

    logger.info("Running Simulation: " + simName)
    // Get Config reference for the simulation
    val simConf = ConfigFactory.load(configName).getConfig("cloudSimulator."+simName)
    logger.info("Loaded config for " + simName)

    logger.trace("Creating Datacenters")
    val dcConfig = simConf.getConfig("datacenters")
    val dataCenters = (1 to dcConfig.getInt("nDcs")).map(i => createDatacenter(dcConfig.getConfig("datacenter" + i), cloudsim))

    logger.trace("Creating Brokers")
    val nBrokers = simConf.getInt("brokers.nBrokers")
    val brokers = (1 to nBrokers).map(i => createBroker(simConf.getConfig("brokers.broker" + i), cloudsim))

    logger.trace("Creating Cloudlets")
    val cloudletConf = simConf.getConfig("cloudlet")
    val cloudletList = (1 to simConf.getInt("nCloudlets")).map( i => createCloudlet(i, cloudletConf))

    logger.trace("Creating VMs")
    val vmConf = simConf.getConfig("vm")
    val vmCreator = new VmCreator(vmConf)
    val vmList = (1 to simConf.getInt("nVms")).map (i => vmCreator.createScalableVm())

    // TODO: Implement multi-broker logic
//    (0 to nBrokers-1).map(i => brokers(i).submitVmList(vmList.asJava))
//    (0 to nBrokers-1).map(i => brokers(i).submitCloudletList(cloudletList.asJava))
    logger.trace("Submitting VMs to broker")
    brokers(0).submitVmList(vmList.asJava)
    logger.trace("Submitting Cloudlets to broker")
    brokers(0).submitCloudletList(cloudletList.asJava)

    logger.info("Starting cloud simulation...")
    cloudsim.start()

    logger.trace("Getting finished cloudlets")
    val finishedCloudlets = (0 to nBrokers-1).map(i => brokers(i).getCloudletFinishedList().asScala).flatten

    logger.info("CloudletScheduling" + vmConf.getString("cloudletScheduler"))

    logger.trace("Building Cloudlet Table")
    val cloudletTable = new CloudletsTableBuilder(finishedCloudlets.asJava)
                            .addColumn(new TextTableColumn("Actual CPU Time", "Usage"), cloudlet => "%.2f".format(cloudlet.getActualCpuTime))
                            .addColumn(new TextTableColumn("CloudletCost", "CPU"), cloudlet => "$ %.2f".format(cloudlet.getActualCpuTime  * cloudlet.getCostPerSec))
                            .addColumn(new TextTableColumn("CloudletCost", "BW"), cloudlet => "$ %.2f".format(cloudlet.getAccumulatedBwCost))
                            .addColumn(new TextTableColumn("CloudletCost", "Total"), cloudlet => "$ %.2f".format(cloudlet.getTotalCost))
                            .addColumn(new TextTableColumn("Utilization %", "CPU"), cloudlet => "%.2f%%".format(cloudlet.getUtilizationOfCpu*100))
                            .addColumn(new TextTableColumn("Utilization %", "RAM"), cloudlet => "%.2f%%".format(cloudlet.getUtilizationOfRam*100))
                            .addColumn(new TextTableColumn("Utilization %", "BW"), cloudlet => "%.2f%%".format(cloudlet.getUtilizationOfBw*100))
                            .addColumn(new TextTableColumn("Utilization %", "BW"), cloudlet => "%.2f%%".format(cloudlet.getUtilizationOfBw*100))

    cloudletTable.build()

    logger.trace("Printing Cloudlet Cost information")
    // Get total and average cloudlet costs
    val cloudletTotalCost = cloudletList.map(i => i.getTotalCost()).sum
    logger.info("Total Cloudlet Cost: " + currencyFormat.format(cloudletTotalCost))
    logger.info("Avg. Cost per Cloudlet: " + currencyFormat.format(cloudletTotalCost/cloudletList.length))

    // Function call to display VM usage costs
    showVmCosts(vmList)

    logger.info("Overall time taken for the execution of the cloudlets is: " + cloudsim.clockStr() + " secs")

  /**
   * Create a broker based on the given config
   *
   * @param brokerConf broker configuration specified
   * @param cloudsim simulation object to which the broker is to be added
   */
  def createBroker(brokerConf: Config, cloudsim: CloudSim): DatacenterBrokerAbstract =
    // Create broker instance based on the type specified in the config
    val brokerType = brokerConf.getString("type")
    val broker = if brokerType == "firstfit" then new DatacenterBrokerFirstFit(cloudsim) else if brokerType == "bestfit" then new DatacenterBrokerBestFit(cloudsim) else new DatacenterBrokerSimple(cloudsim)

    // If time zone matching is requested, add parameter to match VMs to the closest datacenter by timezone
    val brokerMatchesTimezone = if brokerConf.hasPath("matchTimezone") then brokerConf.getString("matchTimezone") else "no"
    if brokerMatchesTimezone == "yes" then broker.setSelectClosestDatacenter(true) else broker.setSelectClosestDatacenter(false)

    broker

  /**
   * Creates a datacenter with the given config
   *
   * @param dcConfig given datacenter configuration
   * @param cloudsim simulation object to which the datacenter is to be added
   */
  def createDatacenter(dcConfig: Config, cloudsim: CloudSim): Datacenter =
    val nHosts = dcConfig.getInt("nHosts")
    logger.trace("Data center config: " + dcConfig)
    // Create list of hosts using config parameters
    val hostList = (1 to dcConfig.getInt("nHosts")).map (i => createHosts(dcConfig.getConfig("host")))
    // Create the datacenter within the cloudsim
    val dc = new DatacenterSimple(cloudsim, hostList.toList.asJava)

    // Set DC name and timezones
    dc.setName(dcConfig.getString("name"))
    dc.setTimeZone(dcConfig.getDouble("timezone"))

    // Set VM allocation policy according to the config
    val vmAllocationPolicy = if dcConfig.hasPath("vmAllocationPolicy") then getVmAllocationPolicy(dcConfig.getString("vmAllocationPolicy")) else new VmAllocationPolicySimple()
    dc.setVmAllocationPolicy(vmAllocationPolicy)

    // Set cost characteristics
    dc.getCharacteristics()
      .setCostPerSecond(dcConfig.getDouble("chars.cpuCost"))
      .setCostPerMem(dcConfig.getDouble("chars.ramCost"))
      .setCostPerStorage(dcConfig.getDouble("chars.storCost"))
      .setCostPerBw(dcConfig.getDouble("chars.bwCost"))

    // Tried to see if it has impact in creation of dynamic VMs
    //dc.setSchedulingInterval(60)

    dc

  /**
   * Create host with the given configuration parameters
   *
   * @param hostConfig given host configuration
   */
  def createHosts(hostConfig: Config): Host =

    // Create host according to the host parameters
    val peMips = hostConfig.getInt("mipsCapacity")/hostConfig.getInt("nPEs")
    val peList = (1 to hostConfig.getInt("nPEs")).map (i => new PeSimple(peMips, new PeProvisionerSimple()))

    val ram:Long = hostConfig.getInt("RAMInMBs")
    val bw:Long = hostConfig.getInt("BandwidthInMBps")
    val storage:Long = hostConfig.getInt("StorageInMBs")
    val ramProvisioner = new ResourceProvisionerSimple()
    val bwProvisioner = new ResourceProvisionerSimple()

    // Get VM scheduler
    val vmScheduler = if hostConfig.hasPath("vmScheduler") then getVmScheduler(hostConfig.getString("vmScheduler")) else new VmSchedulerSpaceShared()

    val host = new HostSimple(ram, bw, storage, peList.asJava)

    host
      .setRamProvisioner(ramProvisioner)
      .setBwProvisioner(bwProvisioner)
      .setVmScheduler(vmScheduler)

    host

  /**
   * Create a cloudlet with the given configuration parameters
   *
   * @param i ID of the cloudlet
   * @param cloudletConf given cloudlet configuration
   */
  def createCloudlet(i: Int, cloudletConf: Config): Cloudlet =

    val utilizationModel = if cloudletConf.hasPath("cloudletUtilModel") then getUtilModel(cloudletConf.getString("cloudletUtilModel")) else new UtilizationModelFull()

    new CloudletSimple(i, cloudletConf.getInt("length"), cloudletConf.getInt("PEs"))
      .setSizes(cloudletConf.getInt("sizes"))
      .setUtilizationModel(utilizationModel.asInstanceOf[UtilizationModel])

  /**
   * Get the utilization model instance corresponding to the specified parameter
   *
   * @param utilModel: given utilization model parameter
   */
  def getUtilModel(utilModel: String): UtilizationModel ={
     if utilModel == "stochastic" then new UtilizationModelStochastic()
        // Creating a dynamic utilization model where utilization updates by 10% everytime an update is called for
        // Based on the example specified in the Cloudsim example implementation
        else if utilModel == "dynamic" then new UtilizationModelDynamic().setUtilizationUpdateFunction(um => um.getUtilization() + um.getTimeSpan()*0.1)
        else UtilizationModelFull()
  }

  /**
   * Get the cloudlet scheduler instance corresponding to the given parameter
   *
   * @param cloudletScheduler: given cloudlet scheduler parameter
   * @see #createListOfScalableVms(int)
   */
  def getCloudletScheduler(cloudletScheduler: String): CloudletSchedulerAbstract = {
    if cloudletScheduler == "time" then new CloudletSchedulerTimeShared() else if cloudletScheduler == "space" then new CloudletSchedulerSpaceShared() else new CloudletSchedulerCompletelyFair()
  }

  /**
   * Get the vm scheduler instance corresponding to the given parameter
   *
   * @param vmScheduler: given vm scheduler parameter
   * @see #createListOfScalableVms(int)
   */
  def getVmScheduler(vmScheduler: String): VmSchedulerAbstract = {
    if vmScheduler == "time" then new VmSchedulerTimeShared() else new VmSchedulerSpaceShared()
  }

  /**
   * Get the vm allocation policy instance corresponding to the given parameter
   *
   * @param vmAllocationPolicy: given vm allocation parameter specified in the config
   * @see #createListOfScalableVms(int)
   */
  def getVmAllocationPolicy(vmAllocationPolicy: String): VmAllocationPolicyAbstract = {
    return (if vmAllocationPolicy == "round" then new VmAllocationPolicyRoundRobin()
            else if vmAllocationPolicy == "best" then new VmAllocationPolicyBestFit()
            else if vmAllocationPolicy == "first" then new VmAllocationPolicyFirstFit()
            else new VmAllocationPolicySimple())
  }

  /**
   * Display the cost of all the vms that was submitted to the broker
   *
   * @param vmList: List of VMs that was created and submitted to the broker
   * @see #createListOfScalableVms(int)
   */
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












