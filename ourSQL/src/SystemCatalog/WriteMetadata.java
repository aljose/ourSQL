package SystemCatalog;

import DatabaseRuntimeProcessor.*;
import StoredDataManager.Main.StoredDataManager;

/*
 * WriteMetadata!
 * Esta clase sirve para escribir informacion dentro de la metadata de main
 * Se escribe nombres de esquemas, nombres de tablas, nombres de consultas ejecutadas y 
 * nombres de columnas con tipo, si es llave primaria y si es llave foranea. 
 */
import java.util.ArrayList;
import DatabaseRuntimeProcessor.InsertInto;
import Shared.Structures.Field;
import Shared.Structures.Row;

/**
 *
 * @author Nicolas Jimenez
 */
public class WriteMetadata {

    /**
     * Anade un nuevo esquema (fila) a la tabla de esquemas de la metadata.
     *
     * @param nombreEsquema
     */
    public void writeEsquema(String nombreEsquema) {

        StoredDataManager inserter = new StoredDataManager();
        inserter.initStoredDataManager("System");

        ArrayList<Field> campo = new ArrayList<Field>();
        Field fielder = new Field(nombreEsquema, "String", false, "SchemaName", "System", true);
        campo.add(fielder);

        Row filas = new Row(campo);

        inserter.insertIntoTable(filas);

    }

    /**
     * Elimina una fila(esquema) de la tabla de esquemas de la metadata
     *
     * @param nombreEsquema
     */
    public void deleteEsquema(String nombreEsquema) {

        StoredDataManager deleter = new StoredDataManager();
        deleter.initStoredDataManager("System");
      
        //  Delete deleter = new Delete ("Schema", "SchemaName" , "=", nombreEsquema );
        //     delete("SchemaName", "Schema", "=", nombreEsquema, "System");
    }

    /**
     * Escribe una nueva fila(tabla) a la tabla de tablas de la metadata
     *
     * @param nombreEsquema
     * @param nombreTabla
     */
    public void writeTabla(String nombreEsquema, String nombreTabla) {

        StoredDataManager inserter = new StoredDataManager();

        inserter.initStoredDataManager("System");
        ArrayList<Field> campo = new ArrayList<Field>();
        Field fieldEsquema = new Field(nombreEsquema, "String", false, "SchemaName", "System", true);
        Field fieldTabla = new Field(nombreTabla, "String", false, "TableName", "System", false);

        campo.add(fieldEsquema);
        campo.add(fieldTabla);
        Row temp = new Row(campo);

        inserter.insertIntoTable(temp);
    }

    /**
     * Elimina una fila (tabla) de la tabla de tablas de la metadata.
     *
     * @param nombreEsquema
     * @param nombreTabla
     */
    public void deleteTabla(String nombreEsquema, String nombreTabla) {

        StoredDataManager deleter = new StoredDataManager();
        deleter.initStoredDataManager("System");
//        delete("Sytem", "Table", "TableName", "=", nombreTabla);
    }

    /**
     * anade una columna (fila) a la tabla de columnas de la metadata
     *
     * @param nombreEsquema
     * @param nombreTabla
     * @param nombreColumna
     * @param tipo
     * @param constraints
     * @param primaryKey
     */
    public void writeColumna(String nombreEsquema, String nombreTabla, String nombreColumna, String tipo,
            String constraints, String primaryKey) {

        StoredDataManager inserter = new StoredDataManager();
        inserter.initStoredDataManager("System");
        ArrayList<Field> campo = new ArrayList<Field>();

        Field fieldE = new Field(nombreEsquema, "String", false, "Schema", "System", true);
        Field fieldTa = new Field(nombreTabla, "String", false, "Table", "System", false);
        Field fieldCol = new Field(nombreColumna, "String", false, "Column", "System", false);
        Field fieldTy = new Field(tipo, "String", false, "Type", "System", false);
        Field fieldCo = new Field(constraints, "String", false, "Constraint", "System", false);
        Field fieldPK = new Field(primaryKey, "String", false, "Primarykey", "System", false);

        campo.add(fieldE);
        campo.add(fieldTa);
        campo.add(fieldCol);
        campo.add(fieldTy);
        campo.add(fieldPK);
        Row temp = new Row(campo);
        inserter.insertIntoTable(temp);
    }

    /**
     * Elimina una columna (fila) de la tabla de columnas de la metadata.
     *
     * @param nombreColumna
     * @param nombreTabla
     * @param nombreEsquema
     */
    public void deleteColumna(String nombreColumna, String nombreTabla, String nombreEsquema) {

        StoredDataManager deleter = new StoredDataManager();
        deleter.initStoredDataManager("System");
        //       delete("System", "Column", "ColumnName", "=", nombreColumna);
    }

    /**
     * Escribe a la tabla de consultas queryLog, una consulta
     *
     * @param nombreConsulta
     * @param nombreTabla
     * @param nombreEsquema
     */
    public void queryLog(String nombreConsulta, String nombreTabla, String nombreEsquema) {

        /*        ArrayList<String> tempValor = new ArrayList();
         tempValor.add(nombreEsquema);
         tempValor.add(nombreTabla);
         tempValor.add(nombreConsulta);

         ArrayList<String> tempCol = new ArrayList();
         tempCol.add("Schema");
         tempCol.add("Table");
         tempCol.add("Query");   */
        StoredDataManager inserter = new StoredDataManager();
        inserter.initStoredDataManager("System");
        ArrayList<Field> campo = new ArrayList<Field>();

        Field schema = new Field(nombreEsquema, "String", false, "Schema", "System", true);
        Field table = new Field(nombreTabla, "String", false, "Table", "System", false);
        Field query = new Field(nombreConsulta, "String", false, "Query", "System", false);

        campo.add(schema);
        campo.add(table);
        campo.add(query);

        Row temp = new Row(campo);
        inserter.insertIntoTable(temp);
        //   InsertInto inserter = new InsertInto();
        //      inserter.executeInsertion("QueryLog", tempCol, tempValor, "System");
    }

    /**
     *
     * @param column
     * @param originTable
     * @param columnReferenced
     * @param tableReferenced
     * @param schema
     */
    public void writerForeignKey(String column, String originTable, String columnReferenced, String tableReferenced,
            String schema) {

        ArrayList<String> tempCol = new ArrayList<String>();
        tempCol.add("Column");
        tempCol.add("OriginTabla");
        tempCol.add("ColumnReferenced");
        tempCol.add("TableReferenced");
        tempCol.add("Schema");

        ArrayList<String> tempVal = new ArrayList<String>();
        tempVal.add(column);
        tempVal.add(originTable);
        tempVal.add(columnReferenced);
        tempVal.add(tableReferenced);
        tempVal.add(schema);

        StoredDataManager inserter = new StoredDataManager();
        inserter.initStoredDataManager("System");

        ArrayList<Field> campo = new ArrayList<Field>();

        Field columna = new Field(column, "String", false, "Column", "System", true);
        Field origen = new Field(originTable, "String", false, "OriginTable", "System", false);
        Field colReferenciada = new Field(columnReferenced, "columnReferenced", false, "Column", "System", false);
        Field tabReferenciada = new Field(tableReferenced, "String", false, "tableReferenced", "System", false);
        Field schemaName = new Field(schema, "String", false, "schema", "System", false);

        campo.add(columna);
        campo.add(origen);
        campo.add(colReferenciada);
        campo.add(tabReferenciada);
        campo.add(schemaName);
        Row temp = new Row(campo);
        inserter.insertIntoTable(temp);
    }

    /**
     * Elimina una llave foranea por esquema
     *
     * @param schema
     */
    public void deleteForeignKey(String schema) {

        StoredDataManager deleter = new StoredDataManager();
        deleter.initStoredDataManager("System");
        //      delete("System", "Schema", "", "=", schema);

    }

}
