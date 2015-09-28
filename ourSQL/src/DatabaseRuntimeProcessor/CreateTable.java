package DatabaseRuntimeProcessor;

/*
 * Create Table
 * Esta clase crea una nueva tabla y con ello crea nuevas columnas, llave primaria y llave foranea.
 * Al hacer esto se debe guardar tanto en disco como en la metadata de urSQL.
 */
import Shared.Structures.Field;
import Shared.Structures.Metadata;
import Shared.Structures.Row;
import StoredDataManager.Main.StoredDataManager;
import SystemCatalog.Constants;
import java.util.ArrayList;

/**
 *
 * @author Nicolas Jimenez
 */
public class CreateTable {

    /**
     *
     * @param database
     * @param nombreTabla
     * @param columns
     */
    public void createTable(String database, String nombreTabla, Row columns) {

        if (!verify(database, nombreTabla)) {
            return;
        }

        addMetadata(database, nombreTabla, columns);
        addTable(database, nombreTabla);

    }

    /**
     * revisa que la tabla no exista.
     *
     * @param database
     * @param nombreTabla
     * @return
     */
    private boolean verify(String database, String nombreTabla) {

        StoredDataManager storer = new StoredDataManager();
        Metadata meta = storer.deserealizateMetadata();

//        ArrayList<ArrayList<String>> metadata = meta.getMetadata().get(Constants.TABLES);
//
//        for (ArrayList<String> fila : metadata) {
//
//            if (fila.get(Constants.TABLE_TABNAME).equals(nombreTabla)
//                    && fila.get(Constants.TABLE_SCHNAME).equals(database)) {
//
//                return false;
//            }
//        }
        return true;

    }

    /**
     * Metodo para anadir la metadata de la recien creada tabla al System
     * Catalog.
     *
     * @param database
     * @param nombreTabla
     * @param columns
     */
    private void addMetadata(String database, String nombreTabla, Row columns) {

        StoredDataManager storer = new StoredDataManager();
        Metadata meta = storer.deserealizateMetadata();

        ArrayList<Field> fields = columns.getColumns();
        Field primerCampo = fields.get(0); //llave primaria

        ArrayList<String> filaInsertar = new ArrayList<>();
        filaInsertar.add(database);
        filaInsertar.add(nombreTabla);
        filaInsertar.add(primerCampo.getContent());

//        meta.getMetadata().get(Constants.TABLES).add(filaInsertar);

    //    ArrayList<ArrayList<ArrayList<String>>> metadata = meta.getMetadata();// variable donder se guarda al final
      //  meta.setMetadata(metadata);
        storer.serializeMetadata(meta);
    }

    /**
     *
     * @param database
     * @param tableName
     */
    private void addTable(String database, String tableName) {

        StoredDataManager temp = new StoredDataManager();
        temp.initStoredDataManager(database);
        temp.createTableFile(tableName);
        temp.flushToDisk();
//        ArrayList<Row>  arrayRow = temp.getAllTuplesFromTable(tableName);
//        for(int i=0;i<arrayRow.size();i++){
//            ArrayList<Field> fieldList= arrayRow.get(i).getColumns();
//            for(int u=0;u<fieldList.size();u++){
//                System.out.print(fieldList.get(u).getContent()+"   ");
//            }
//            System.out.println();
//        }
     
    }

}
