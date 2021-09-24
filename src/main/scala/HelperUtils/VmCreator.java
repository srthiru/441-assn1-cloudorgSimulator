package HelperUtils;

import Simulations.CloudOrgSim$;
import com.typesafe.config.Config;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerAbstract;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerCompletelyFair;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.autoscaling.HorizontalVmScaling;
import org.cloudsimplus.autoscaling.HorizontalVmScalingSimple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;

public class VmCreator {

    // Creating logger instance
    private final Logger logger = LoggerFactory.getLogger(CloudOrgSim$.class);
    private int vms;
    private Config vmConf;

    public VmCreator(Config vmConf){
        this.vmConf = vmConf;
        this.vms = 0;
    }

    /**
     * Creates a Scalable Vm object that is able to scale horizontally when overloaded
     *
     * @return the created Vm
     */
    public Vm createScalableVm(){
        final Vm vm =  createVm();

        if(vmConf.hasPath("scalingEnabled") && vmConf.getString("scalingEnabled") == "yes"){
            final String scalingType = vmConf.hasPath("scalingType")? vmConf.getString("scalingType"): "horizontal";

            if(scalingType == "horizontal"){
                logger.info("Enabling horizontal scaling for VM "+ vm.getId());
                createHorizontalVmScaling(vm);
            }
            else{
                logger.warn("Scaling type not implemented, defaulting to horizontal scaling");
                logger.info("Enabling horizontal scaling for VM "+ vm.getId());
                createHorizontalVmScaling(vm);
            }
        }
        return vm;
    }

    /**
     * Creates a Vm object.
     *
     * @return the created Vm
     */
    private Vm createVm() {
        final int id = vms++;
        final CloudletSchedulerAbstract scheduler = vmConf.hasPath("cloudletScheduler")? getCloudletScheduler(vmConf.getString("cloudletScheduler")) : new CloudletSchedulerSpaceShared();

        final Vm vm =  new VmSimple(id, vmConf.getInt("mipsCapacity"), vmConf.getInt("PEs"))
                            .setRam(vmConf.getInt("RAMInMBs")).setBw(vmConf.getInt("BandwidthInMBps")).setSize(vmConf.getInt("StorageInMBs"))
                            .setCloudletScheduler(scheduler).setTimeZone(vmConf.getDouble("timezone"));

        logger.info("Created VM " + vm.getId() + " in datacenter " + vm.getHost().getDatacenter().getName());

        return vm;
    }

    private CloudletSchedulerAbstract getCloudletScheduler(String scheduler){
        switch(scheduler){
            case "time":
                return new CloudletSchedulerTimeShared();
            case "space":
                return new CloudletSchedulerSpaceShared();
            default:
                return new CloudletSchedulerCompletelyFair();
        }
    }

    /**
     * A {@link Predicate} that checks if a given VM is overloaded or not,
     * based on upper CPU utilization threshold.
     * A reference to this method is assigned to each {@link HorizontalVmScaling} created.
     *
     * @param vm the VM to check if it is overloaded
     * @return true if the VM is overloaded, false otherwise
     * @see #createHorizontalVmScaling(Vm)
     */
    private boolean isVmOverloaded(final Vm vm) {
        return vm.getCpuPercentUtilization() > 0.7;
    }

    /**
     * Creates a {@link HorizontalVmScaling} object for a given VM.
     *
     * @param vm the VM for which the Horizontal Scaling will be created
     * @see #createListOfScalableVms(int)
     */
    private void createHorizontalVmScaling(final Vm vm){
        final HorizontalVmScaling horizontalScaling = new HorizontalVmScalingSimple();
        horizontalScaling
                .setVmSupplier(this::createVm)
                .setOverloadPredicate(this::isVmOverloaded);
        vm.setHorizontalScaling(horizontalScaling);
    }

}
