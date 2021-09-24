package HelperUtils;

import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.builders.tables.TableColumn;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

public class CustomCloudletsTable extends CloudletsTableBuilder {

    private static NumberFormat format = NumberFormat.getCurrencyInstance(Locale.US);

    /**
     * Instantiates a builder to print the list of Cloudlets using the a
     * default {@link MarkdownTable}.
     * To use a different {@link Table}, check the alternative constructors.
     *
     * @param list the list of Cloudlets to print
     */
    public CustomCloudletsTable(final List<? extends Cloudlet> list) {
        super(list);
    }

    /**
     * Instantiates a builder to print the list of Cloudlets using the
     * given {@link Table}.
     *
     * @param list the list of Cloudlets to print
     * @param table the {@link Table} used to build the table with the Cloudlets data
     */
    @Override
    protected void createTableColumns() {
        super.createTableColumns();

        // TODO: Figure out the different cost components and add to the table
        TableColumn colCpuCost = getTable().addColumn("Cost", "CPU");
        addColumnDataFunction(colCpuCost, cloudlet -> format.format( cloudlet.getActualCpuTime() * cloudlet.getCostPerSec()));

//        TableColumn colBwCost = getTable().addColumn("Cost", "BW");
//        addColumnDataFunction(colBwCost, cloudlet -> format.format(cloudlet.getUtilizationOfRam() * cloudlet.getVm()));

        TableColumn colBwCost = getTable().addColumn("Cost", "BW");
        addColumnDataFunction(colBwCost, cloudlet -> format.format(cloudlet.getAccumulatedBwCost()));

        TableColumn colTotalCost = getTable().addColumn("Cost", "Total");
        addColumnDataFunction(colTotalCost, cloudlet -> format.format(cloudlet.getTotalCost()));

    }

}