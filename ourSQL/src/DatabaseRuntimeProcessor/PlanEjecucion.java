package DatabaseRuntimeProcessor;

import java.util.ArrayList;

/*
 * Clase que se encarga de realizar el plan de ejecucion.
 * Lo guarda por ahora con DatabaseRuntimeProcessor.WRFiles.
 */
/**
 *
 * @author Nicolas Jimenez
 */
public class PlanEjecucion {

    private final String comando;
    private final ArrayList<String> instruccion;
    private final ArrayList<String> plan;
    private final WRFiles archivos;

    public PlanEjecucion(String comando, ArrayList<String> instruccion) {

        this.archivos = new WRFiles();
        this.comando = comando;
        this.instruccion = instruccion;
        this.plan = new ArrayList<String>();
        procesar();
    }

    /**
     * Procesa segun el tipo de comando introducido.
     *
     * @return
     */
    public void procesar() {

        if (comando.equals("createDatabase")) {

             createDatabase();
             
        } else if (comando.equals("dropDatabase")) {

            dropDatabase();

        } else if (comando.equals("listDatabases")) {

            listDatabases();

        } else if (comando.equals("stop")) {

            //  return stop;
            return;

        } else if (comando.equals("start")) {

            return ;

        } else if (comando.equals("getStatus")) {

            return ;

        } else if (comando.equals("display")) {

            display();
            
        } else if (comando.equals("update")) {
            
            update();

        } else if (comando.equals("set")) {

            set();

        } else if (comando.equals("createTable")) {

             createTable();

        } else if (comando.equals("alter")) {

            alter();

        } else if (comando.equals("dropTable")) {

            dropTable();

        } else if (comando.equals("createIndex")) {

            createIndex();

        } else if (comando.equals("select")) {

            select();

        } else if (comando.equals("delete")) {

            delete();

        } else if (comando.equals("insert")) {

            insert();
        
        } else {

            System.err.print("Error invalid command");
            System.out.println("should not have happened");
        }
    }

    /**
     * @return
     */
    private void createDatabase() {

        String line1 = "Se analiza que la instruccion de create database este bien formulada.";
        String line2 = "Se crea una carpeta en el sistema de archivos para guardar las tablas de la base de datos: "
                + instruccion.get(2) + ".";
        String line3 = "Se guarda la informacion de la base de datos la metadata del main.";
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void dropDatabase() {

        String line1 = "Se analiza que la instruccion de drop database este bien formulada.";
        String line2 = "Se elimina la  carpeta en el sistema de archivos que almacena las tablase de la base de datos: "
                + instruccion.get(2) + ".";
        String line3 = "Se elimina la iinformacion relacionada de " + instruccion.get(2) + " de la metadata.";
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void listDatabases() {

        String line1 = "Se analiza que la instruccion de list databases este bien formulada";
        String line2 = "Se busca en la metadata las bases de datos creadas";
        String line3 = "Con esa información se genera un listado de todos los esquemas existentes. ";
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * Wish I know.
     */
//    private ArrayList<String> stop() {
    //   }
    /**
     * @return
     */
    //   private ArrayList<String> getStatus() {
//    }
    /**
     * @return
     */
    private void display() {

        String line1 = "Se analiza que la instruccion de display database este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la base de datos: "
                + instruccion.get(2);
        String line3 = "Con base en la informacion obtenida se muestra de alguna forma las tablas y columnas del "
                + "esquema " + instruccion.get(2);
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     *
     */
    private void set() {

        String line1 = "Se analiza que la instruccion de set este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la base de datos: "
                + instruccion.get(2);
        String line3 = "Con base en la informacion obtenida se muestra de alguna forma las tablas y columnas del "
                + "esquema " + instruccion.get(2);
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void createTable() {

        String line1 = "Se analiza que la instruccion de create table este bien formulada";
        String line2 = "Se procede a crear una tabla en el esquema establecido anteriormente para esto se extrae "
                + "informacion de la instruccion como las columnas, la llave primaria y las restricciones y se guarda en la metadata";
        String line3 = "Tambien se crea un arbol en donde se almacena toda la informacion de la tabla "
                + instruccion.get(2);
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void alter() {
        String line1 = "Se analiza que la instruccion de alter  este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la base de datos: "
                + instruccion.get(2);
        String line3 = "Con base en la informacion obtenida se agrega el constraint de una llave foranea en la metadata"
                + " de la tabla " + instruccion.get(2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void createIndex() {

        String line1 = "Se analiza que la instruccion de create index este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la tabla: "
                + instruccion.get(4) + ". Tambien se crea un nuevo HASH sobre esa tabla ";
        String line3 = "Finalmente se guarda con el Stored Data Manager el nuevo indice y tambien en el System Manager "
                + " para realizar las busquedas sobre la columna  " + instruccion.get(6);
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void select() {

        String line1 = "Se analiza que la instruccion de select  este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la tabla y/o conjunto de tablas"
                + " con join, tambien se revisan las condiciones";
        String line3 = "Luego se revisa toda la informacion necesaria en el arbol B+ gracias al Stored Data Manager "
                + "para cumplir con las condiciones";
        String line4 = "Se ejecuta la creacion de un xml o de un json dependiendo del caso";
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        plan.add(line4);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void update() {

        String line1 = "Se analiza que la instruccion de update este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la tabla: "
                + instruccion.get(1) + ". Se busca la columna especificada " + instruccion.get(3);
        String line3 = "Dependiendo de la declaracion where se actualizan las filas necesarias en el arbol B+";
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void delete() {

        String line1 = "Se analiza que la instruccion de delete este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la tabla: "
                + instruccion.get(1);
        String line3 = "Dependiendo de la declaracion where se actualizan las filas necesarias en el arbol B+";
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    private void insert() {

        String line1 = "Se analiza que la instruccion de insert into este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la tabla: "
                + instruccion.get(2) + ". Se buscan las columnas especificadas ";
        String line3 = "Se busca la tabla y las columnas en el arbol B+ y se crea una nueva fila con los valores indicados";
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }

    /**
     * @return
     */
    //   private ArrayList<String> start() {
    // }
    /**
     * @return
     */
    private void dropTable() {

        String line1 = "Se analiza que la instruccion de drop table este bien formulada";
        String line2 = "Se busca en la metadata del System Catalog la informacion sobre la tabla: "
                + instruccion.get(2);
        String line3 = "Se elimina el arbol B+ asociado a esta tabla " + instruccion.get(2);
        plan.add(line1);
        plan.add(line2);
        plan.add(line3);
        archivos.writer("PlanDeEjecucion", plan);
    }
}
