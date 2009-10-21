package opennlp.textgrounder.util;

import java.io.*;

/**
 * Handy methods for interacting with the file system and running commands.
 *
 * @author     Jason Baldridge
 * @created    April 15, 2004
 */
public class IOUtil {

    /**
     * Write a string to a specified file.
     *
     * @param  contents  The string containing the contents of the file
     * @param  outfile   The File object identifying the location of the file
     */
    public static void writeStringToFile (String contents, File outfile) {
	try {
	    BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
	    bw.write(contents);
	    bw.flush();
	    bw.close();
	} catch (IOException ioe) {
	    System.out.println("Input error writing to " + outfile.getName());
	}
	return;
    }
    
    /**
     * Calls runCommand/2 assuming that wait=true.
     *
     * @param  cmd  The string containing the command to execute
     */
    public static void runCommand (String cmd) {
	runCommand(cmd, true);
    }

    /**
     * Run a command with the option of waiting for it to finish.
     *
     * @param  cmd  The string containing the command to execute
     * @param  wait True if the caller should wait for this thread to 
     *              finish before continuing, false otherwise.
     */
    public static void runCommand (String cmd, boolean wait) {
	try {
            System.out.println("Running command: "+ cmd);
	    Process proc = Runtime.getRuntime().exec(cmd);

	    // This needs to be done, otherwise some processes fill up
	    // some Java buffer and make it so the spawned process
	    // doesn't complete.
            BufferedReader br = 
		new BufferedReader(new InputStreamReader(proc.getInputStream()));
            
            String line = null;
            while ((line = br.readLine()) != null) {
            // while (br.readLine() != null) {
		// just eat up the inputstream

		// Use this if you want to see the output from running
		// the command.
		System.out.println(line);
	    }

	    if (wait) {
		try {
		    proc.waitFor();
		} catch (InterruptedException e) {
		    Thread.currentThread().interrupt();
		}
	    }
	    proc.getInputStream().close();
	    proc.getOutputStream().close();
	    proc.getErrorStream().close();
	} catch (IOException e) {
	    System.out.println("Unable to run command: "+cmd);
	}
    }

}
