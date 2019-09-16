package challenge;
import java.io.File;
import java.io.IOException;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;

import utility.ETLUtility;

/**
 * The ReadBigDataFile class read large flat files 
 * and performing searches, filters and translations, 
 * resulting in a new file with occurrences 
 * found in the same template.
 * @author leonardomoraes
 * @version 07/09/2019 
 */
public class ReadBigDataFile {
	
	final static String finalFileName = "ResultFinal.txt";

	public static void main (String[] args) throws IOException {
		
		JFileChooser chooser = new JFileChooser();
		chooser.setMultiSelectionEnabled(true);
		chooser.setDialogTitle("SELECT THE FLAT FILES");

		if(chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
			
			//LOAD ALL FLAT FILES
			File[] files = chooser.getSelectedFiles();
			chooser.setSelectedFile(new File(""));
			
			//LOAD FIRST CONFIGURATION		
			String[][] conf1 = null;
			chooser.setDialogTitle("SELECT THE FIRST(COLUMN) CONFIGURATION FILE");
			chooser.setMultiSelectionEnabled(false);
			if(chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
				conf1 = ETLUtility.extract(chooser.getSelectedFile().getAbsolutePath());
				chooser.setSelectedFile(new File(""));
			} else {
				JOptionPane.showMessageDialog(null, "First configuration file not selected. :(");
				System.exit(0);
			}
			
			//LOAD SECOND CONFIGURATION
			String[][] conf2 = null;
			chooser.setDialogTitle("SELECT THE SECOND(ROW) CONFIGURATION FILE");
			chooser.setMultiSelectionEnabled(false);
			if(chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
				conf2 = ETLUtility.extract(chooser.getSelectedFile().getAbsolutePath());
				chooser.setSelectedFile(new File(""));
			} else {
				JOptionPane.showMessageDialog(null, "Second configuration file not selected. :(");
				System.exit(0);
			}
			
			chooser = null;
			
			// MAKE ALL TRANSFORMATION FOR ALL FLAT FILE SELECTED
			for (File file : files) {

				ETLUtility.createFilteredFile(file, conf1, conf2, finalFileName);
/**				String[][] board = ETLUtility.extract(file.getAbsolutePath());

				String[][] firstFilter = ETLUtility.transformationColumns(board, conf1);

				String[][] secondFilter = ETLUtility.transformationRows(firstFilter, conf2);

				ETLUtility.loadFilteredFile(finalFileName, secondFilter);
				*/
			}
			
			JOptionPane.showMessageDialog(null, "File: "+finalFileName+" successfully created/updated ");


		} else {
			JOptionPane.showMessageDialog(null, "No flat files have been selected. :(");
		}
		
		System.exit(0);
	}

}