package utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


/**
 * Utility class responsible for helping 
 * extraction, transformation and loading
 * huge datas from flat files.
 * @author lemoraes
 */
public final class ETLUtility {

	// Private constructor to prevent instantiation
	private ETLUtility() {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Method responsible of couting rows and columns 
	 * from flat file
	 * @author leonardomoraes
	 * @param nomeArquivo
	 * @return int[] - Total of rows and columns for dynamic flat file
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private static int[] countRowAndColumn(String nomeArquivo) throws IOException, FileNotFoundException {
		
		String readLine = "";
		int lines = 0;
		int columns = 0;

		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(new FileReader(nomeArquivo));
			
			while ((readLine = reader.readLine()) != null) {
				
				if (lines == 0) {
					String separador[] = readLine.split("\\t");
					columns = separador.length;
				}
				lines++;
			}
			
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
	        assert reader != null;
	        reader.close();
		}
		
		return new int[] {lines, columns};
		
	}
	
	/**
	 * Method responsible of extract all data from dynamic flat file
	 * and keep in memory through two dimensional string array
	 * @author leonardomoraes
	 * @param boardFileName - Filename to be loaded 
	 * @return String[][] - Two Dimensional String Array with all data for the file.
	 * @throws IOException
	 */
	public static String[][] extract(String boardFileName) throws IOException {
		
		int[] linhaColuna = countRowAndColumn(boardFileName);
		
	    String[][] board = new String[linhaColuna[0]][linhaColuna[1]];

	    BufferedReader reader = null;

	    try {
	        reader = new BufferedReader(new FileReader(boardFileName));

	        for(int i = 0; i < linhaColuna[0] && reader.ready(); i++) {
	            String[] splittedRow = reader.readLine().split("\\t"); // split using the TAB
	            for(int j = 0; j < linhaColuna[1]; j++) {
	                board[i][j] = splittedRow[j];
	            }
	        }

	        return board;

	    } catch(IOException e) {
	        throw e;
	    } finally {
	        assert reader != null;
	        reader.close();
	    }
	}
	
	
	public static void createFilteredFile(File boardFileName, String[][] configurationColumn, String[][] configurationRow, String outputFile) throws IOException{
		 File file = new File(outputFile);
		 BufferedReader reader = null;
		 BufferedWriter bufferedWriter = null;
		 try {
	        reader = new BufferedReader(new FileReader(boardFileName));
	        boolean append = file.exists();
			bufferedWriter = new BufferedWriter(new FileWriter(outputFile, append));
	        
	        boolean firstLine = true;
	        StringBuilder lineBuilder;
	        ArrayList<Integer> colNumber = new ArrayList<Integer>(configurationColumn.length);
	        //Read all lines of file
	        for(String line = reader.readLine(); line != null; line = reader.readLine()){
	        	String[] splittedRow = line.split("\\t");
	        	lineBuilder = new StringBuilder();
	        	//Checking column names line
	        	if(firstLine){
	        		firstLine = false;
		        	for(int j = 0; j < splittedRow.length; j++) {
		        		for(int l = 0; l < configurationColumn.length; l++){
					        if (splittedRow[j].equals(configurationColumn[l][0])) {
					        	if(!append){
						        	if(!colNumber.isEmpty()){
						        		lineBuilder.append("\t");
						        	}
						        	lineBuilder.append(configurationColumn[l][1]);
						        }
					        	//Column allowed to insert
					        	colNumber.add(j);
					        }
		        		}
		            }
//		        	if(!append){
//		        		bufferedWriter.newLine();
//		        	}
	        	}else{
	        		//Checking the data lines
	        		boolean foundRowId = false;
	        		//Checking if ID is mapped
	        		for(int l = 0; l < configurationRow.length; l++){
				        if (splittedRow[0].equals(configurationRow[l][0])) {
				        	lineBuilder.append(configurationRow[l][1]);
				        	foundRowId = true;
				        	break;
				        }
	        		}
	        		//If ID is mapped, keep inserting
	        		if(foundRowId){
	        			for(int j = 1; j < splittedRow.length; j++) {
	        				//check if Column is allowed to insert
		        			if(colNumber.contains(j)){
		        				lineBuilder.append("\t");
		        				lineBuilder.append(splittedRow[j]);	
		        			}
			            }
	        			bufferedWriter.newLine();
	        		}
	        	}
	        	if(lineBuilder.length() > 0){
	        		bufferedWriter.write(lineBuilder.toString());
	        	}
	        }
	        
	    } catch(IOException e) {
	       throw e;
	    } finally {
	        assert reader != null;
	        reader.close();
	        bufferedWriter.close();
	    }
	}

	/**
	 * Method responsible for finding data and translating it 
	 * using the first configuration file as a filter.
	 * @author leonardomoraes
	 * @param String[][] board - 2D String Array with all data
	 * @param String[][] configurationColumn - 2D Array with the first configuration 
	 * @return String[][] - 2D String Array with all
	 * the filtered and translated columns 
	 */
	public static String[][] transformationColumns(String[][] board, String[][] configurationColumn){
		
		String[][] firstFilter = new String[board.length][configurationColumn.length];
		
		for (int i = 0; i < board.length; i++) {
		    for (int j = 0; j < board[0].length; j++) {
		    			    	
		    	for(int l = 0; l < configurationColumn.length; l++)
		    	
			        if (board[i][j].equals(configurationColumn[l][0])) {
			        	
			        	firstFilter[0][l] = configurationColumn[l][1];
			        	
			        	for (int k = 1; k < board.length; k++) {
			        		firstFilter[k][l] = board[k][j];
						}
			        }
		    }
		}
		return firstFilter;
	}
	
	/**
	 * Method responsible for finding data with 
	 * already filtered columns and translating it
	 * using the second configuration file as a filter.
	 * @author leonardomoraes
	 * @param String[][] firstFilter - 2D Array with data filtered
	 * @param String[][] configurationRows - 2D Array with second configuration
	 * @return String[][] - 2D String Array with all
	 * the filtered and translated rows 
	 */
	public static String[][] transformationRows(String[][] firstFilter, String[][] configurationRows){
		
		String[][] secondFilter = new String[configurationRows.length + 1][firstFilter[0].length];

		for (int i = 0; i < firstFilter.length; i++) {

			System.arraycopy(firstFilter, 0, secondFilter, 0, 1);

			for (int j = 0, aux = 1; j < configurationRows.length; j++, aux++) {

				if (firstFilter[i][0].equals(configurationRows[j][0])) {
					firstFilter[i][0] = configurationRows[j][1];
					System.arraycopy(firstFilter, i, secondFilter, aux, 1);
				}

			}
		}
		return secondFilter;
	}
	
	/**
	 * Method responsible for creating the final
	 * filtered and translated flat file
	 * @author leonardomoraes
	 * @param fileName - Final file name
	 * @param finalFilter - Final Filter Data to output
	 * @throws FileNotFoundException
	 */
	public static void loadFilteredFile(String fileName, String[][] finalFilter){

		File file = new File(fileName);
			
		boolean append = false;
		int ix = 0;
		if(file.exists()) {
			append = true;
			ix = 1;
		}

		//try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName))){
		try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName,append))){
			
			for (int i = ix; i < finalFilter.length; i++) {
				
				if(finalFilter[i][0] != null) {
				
					for (int j = 0; j < finalFilter[0].length; j++) {
							bufferedWriter.write(finalFilter[i][j] + "	");
							System.out.print(finalFilter[i][j] + " ");
					}
					if (i < finalFilter.length)
						bufferedWriter.newLine();
				} else {
					continue;
				}

				//ocurrence++;
				System.out.println();
			}

			bufferedWriter.close();
		}catch (IOException e){
	            e.printStackTrace();

		}

	}
		

}
