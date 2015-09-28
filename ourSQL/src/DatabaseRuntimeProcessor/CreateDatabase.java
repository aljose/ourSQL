package DatabaseRuntimeProcessor;

/*
 * Esta clase de DatabaseRuntimeProcessor.CreateDatabase se encarga de crear un esquema  (base de datos) 
 * al hacer esto se agrega el esquema nuevo.
 * Este se agrega a la metadata del urSQL y tambien se crea una carpeta en el sistema de archivos donde se 
 * agregaran las tablas. 
 *
 */
import java.util.ArrayList;
import Shared.Structures.Metadata;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;

/**
 *
 * @author Nicolas Jimenez
 */
public class CreateDatabase {

    public void createDatabase(String dataBase) {

        if (!verifyExist(dataBase)) {
            System.out.println("No se puede crear la base de datos");
            return;
        }
        //addMetadata(dataBase);
        addSchema(dataBase);
    }

    /**
     * Verifica que la base de datos no exista.
     *
     * @param dataBaseSchemas
     * @param dataBase
     * @return
     */
    private boolean verifyExist(String dataBase) {

        StoredDataManager storer = new StoredDataManager();
        Metadata meta = storer.deserealizateMetadata();

//        ArrayList<ArrayList<String>> metadata = meta.getMetadata().get(Constants.SCHEMA);

//        for (ArrayList<String> fila : metadata) {
//
//            for (String campo : fila) {
//
//                if (campo.equals(dataBase)) {
//                    return false;
//                }
//            }
//        }
        return true;
    }

    /**
     * Agrega la metadata del nuevo esquema.
     *
     * @param databaseName
     */
    private void addMetadata(String databaseName) {

        StoredDataManager storer = new StoredDataManager();
        Metadata meta = storer.deserealizateMetadata();
        ArrayList<String> schemaTable = new ArrayList<>();

        schemaTable.add(databaseName);

        meta.getMetadata().get(Constants.SCHEMA).add(schemaTable);

        ArrayList<ArrayList<ArrayList<String>>> metadata = meta.getMetadata();// variable donder se guarda al final
        meta.setMetadata(metadata);
        storer.serializeMetadata(meta);
    }

    /**
     * Agrega el esquema nuevo.
     *
     * @param databaseName
     */
    private void addSchema(String databaseName) {

        StoredDataManager temp = new StoredDataManager();
        temp.createDatabase(databaseName);
    }
}
