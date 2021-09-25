package Simulations

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SimulationCheckerTestSuite  extends AnyFlatSpec with Matchers {
  behavior of "configuration parameters module"

  val conf = ConfigFactory.load().getConfig("cloudSimulator")

  it should "contain atleast one simulation" in {
    assert(conf.getInt("nSimulations") >= 1)
  }

  it should "contain parameters for all basic components" in {
    (1 to conf.getInt("nSimulations")).map( i => assert(conf.hasPath("cloudSimulator.simulation"+i+".datacenters")))//
    (1 to conf.getInt("nSimulations")).map(i => assert(conf.hasPath("cloudSimulator.simulation"+i+".vm")))
    (1 to conf.getInt("nSimulations")).map(i => assert(conf.hasPath("cloudSimulator.simulation"+i+".cloudlet")))
  }

  it should "have VM mips capacity to be atleast 1000" in {
    (1 to conf.getInt("nSimulations")).map(i => assert(conf.getInt("cloudSimulator.simulation"+i+".vm.mipsCapacity") >= 1000))
  }
}
