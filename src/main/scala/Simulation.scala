import HelperUtils.{CreateLogger, ObtainConfigReference}
import Simulations.{BasicCloudSimPlusExample, CloudOrgSim} //CloudletSchedulerSpaceSharedExample
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Simulation:
  val logger = CreateLogger(classOf[Simulation])

  @main def runSimulation =
    logger.info("Constructing a cloud model...")
    CloudOrgSim.runSim("simulation1", "application.conf")
//    CloudletSchedulerSpaceSharedExample.Start()
    logger.info("Finished cloud simulation...")

class Simulation