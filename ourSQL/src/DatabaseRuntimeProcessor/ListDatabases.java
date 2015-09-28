package DatabaseRuntimeProcessor;

import Shared.Structures.Metadata;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;
import java.util.ArrayList;

/**
 *
 * @author Kevin
 */
public class ListDatabases {

    StoredDataManager dataManager;

    /**
     *
     *
     * @return String specifying the databases stored in the System Catalog.
     *
     */
    public String listDatabases() {
        Metadata metadata = new Metadata();
        metadata = dataManager.deserealizateMetadata();
        ArrayList<ArrayList<ArrayList<String>>> met = metadata.getMetadata();
        ArrayList<ArrayList<String>> schemaTable = met.get(Constants.SCHEMA);
        ArrayList<String> row;
        String elemento;
        StringBuilder buildResult = new StringBuilder();
        buildResult = buildResult.append("Las bases de datos almacenadas en el sistema son: \n");
        buildResult = buildResult.append("Nombre del esquema \n");
        for (int i = 1; i < schemaTable.size(); i++) {
            row = schemaTable.get(i);
            elemento = row.get(Constants.SCHEMA_SCHNAME);
            buildResult = buildResult.append(elemento);
            buildResult = buildResult.append("\n");
        }
        return buildResult.toString();
    }
}
