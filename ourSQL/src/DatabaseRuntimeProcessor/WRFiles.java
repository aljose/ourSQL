package DatabaseRuntimeProcessor;
/*
 *Clase que se encarga de escribir y leer archivos planos del plan de ejecucion 
 * Es temporal hasta que se pueda almacenar en el arbol B+
 */

import java.io.*;
import java.util.ArrayList;

/**
 *
 * @author nicolasjimenez
 */
public class WRFiles {

    /**
     * Escribe un plan de ejecucion linea por linea en un archivo plano.
     *
     * @param fileName
     * @param input
     */
    public void writer(String fileName, ArrayList<String> input) {

        try {
            // Assume default encoding.
            FileWriter fileWriter = new FileWriter(fileName);

            // Note that write() does not automatically
            // append a newline character.
            try { // Always wrap FileWriter in BufferedWriter.
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter); {
                // Note that write() does not automatically
                // append a newline character.
                for (String line : input) {

                    bufferedWriter.write(line);
                    bufferedWriter.newLine();
                }
                bufferedWriter.close();

                // Always close files.

        }} catch (IOException ex) {
            System.out.println("Error writing to file '" + fileName + "'");
        }
    } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Clase lee un archivo y lo almacena linea por linea dentro de un ArrayList
     * @param fileName
     * @return
     */
    public ArrayList<String> reader(String fileName) {

        // This will reference one line at a time
        String line= null;
        ArrayList<String> output = new ArrayList();
        int i = 0;
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            try { // Always wrap FileReader in BufferedReader.
                    BufferedReader bufferedReader = new BufferedReader(fileReader); {
                while ((line = bufferedReader.readLine()) != null) {
                    output.add(line);
                    System.out.println(line);
                }
                // Always close files.
            }
        } catch (FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + "'");
        } catch (IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");
        }
        return output;
    } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return output;
    }}
