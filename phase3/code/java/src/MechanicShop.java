/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		try {
                	System.out.print("Create ID: ");
                        String id = in.readLine();
			System.out.print("First name: "); 
                	String fname = in.readLine();
			System.out.print("Last name: ");
			String lname = in.readLine();
			System.out.print("Phone Number: ");
			String phone = in.readLine();
			System.out.print("Address: ");
			String address = in.readLine();

			String customerInfo = "INSERT INTO Customer VALUES ('" +  id + "' , '" + fname + "' , '" + lname + "' , '" + phone + "' , '" + address + "')";
			
			esql.executeUpdate(customerInfo);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}	
		
	public static void AddMechanic(MechanicShop esql){//2
		try {
			System.out.print("Create ID: ");
                        String id = in.readLine();
                        System.out.print("First name: ");
                        String fname = in.readLine();
                        System.out.print("Last name: ");
                        String lname = in.readLine();
                        System.out.print("Years of experience: ");
                        String experience = in.readLine();

                        String mechanicInfo = "INSERT INTO Mechanic VALUES ('" + id + "' , '"  + fname + "' , '" + lname + "' , " + experience + ")";

                        esql.executeUpdate(mechanicInfo);
                }
                catch (Exception e) {
                        System.out.println(e.getMessage());
                }
	}
	
	public static void AddCar(MechanicShop esql){//3
		try{
			System.out.print("VIN: ");
			String vin = in.readLine();
			String validateVin = "SELECT * FROM Car WHERE Car.vin = '" + vin +  "'";

			if(esql.executeQuery(validateVin) > 0) {
				System.out.println("The vin you inputted already exists");
			return;
			}

			System.out.print("Make: ");
			String make = in.readLine();
			System.out.print("Model: ");
	 	        String model = in.readLine();
	        	System.out.print("Year: ");
	        	String year = in.readLine();

	        	String carInfo = "INSERT INTO Car VALUES ('" + vin + "' , '" + make + "' , '" + model + "' , " + year + ")";
	        
	        	esql.executeUpdate(carInfo);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}		
	}		
	
	public static void InsertServiceRequest(MechanicShop esql){//4
	        try {
        		System.out.print("Enter last name: ");
        		String lname = in.readLine();
        
        		String getName = "Select fname, lname, id From Customer Where lname = '" + lname + "'";

        		List<List<String>> nameResults = esql.executeQueryAndReturnResult(getName);
			System.out.println(nameResults);
  
        		if (esql.executeQuery(getName) == 0) {
        			System.out.print("ERROR: Customer not found");
        			AddCustomer(esql); 
        			return;        
        		}
       
			System.out.println ("Choose a customer id: ");
            		String custId = in.readLine();
          
            
        		String getCar = "SELECT car_vin FROM Owns WHERE customer_id = " + custId;
			List<List<String>> carResult = esql.executeQueryAndReturnResult(getCar);
          		System.out.println(carResult);

        		String UserDecision = "";
        		while(!UserDecision.equals("0") && !UserDecision.equals("0"))
        		{
            		System.out.print("Enter 0 to choose a listed car OR enter 1 to add a new car\n");
            		UserDecision = in.readLine();
   			}
        		String vin  = "";
            
    			if (UserDecision.equals("1"))
      			{
        			AddCar(esql);
        			return;
		        }
        		else if(UserDecision.equals("0"))
        		{

            		System.out.println ("Choose vin: ");
            		vin = in.readLine();
        
            		System.out.println ("Create new service request: ");
            
            		System.out.println("Enter date: ");
            		String date = in.readLine();
             
            		System.out.println("Enter odometer reading: ");
            		String odometer = in.readLine();
             
			System.out.println("Enter complaint: ");
         		String complaint = in.readLine();
             
            		String query = "INSERT INTO Service_Request (customer_id, car_vin, date, odometer, complain) VALUES ( " + "'" + custId + "' , '" + vin + "' , '" + date + "' , '" + odometer + "' , '" + complaint + "')";
            		esql.executeUpdate(query);
        		}  
            
      		  } catch(Exception e){
           		 System.err.println (e.getMessage());
        	  }
        
	}

	
	public static void CloseServiceRequest(MechanicShop esql){//5	
	
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try{
			String query = "SELECT CR.date, CR.comment, CR.bill FROM Closed_Request CR WHERE CR.bill < 100;";
			int result = esql.executeQueryAndPrintResult(query);
			System.out.println("result: " + result);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try{
			String query = "SELECT cars.fname, cars.lname, cars.num_of_cars FROM (SELECT O.customer_id, C.fname, C.lname, COUNT(*) num_of_cars FROM Owns O, Customer C WHERE C.id = O.customer_id GROUP BY O.customer_id, C.fname, C.lname) AS cars WHERE num_of_cars > 20";

			int result = esql.executeQueryAndPrintResult(query);
                        System.out.println("result: " + result);
		}
		catch(Exception e){
			System.out.println(e.getMessage());
		}		
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try{
         		String query = "SELECT C.make, C.model, C.year FROM Car C, Service_Request SR WHERE C.vin = SR.car_vin AND C.year < 1995 AND SR.odometer < 50000";
			
			int result = esql.executeQueryAndPrintResult(query);
                        System.out.println("result: " + result);
                }
                catch(Exception e){
                        System.out.println(e.getMessage());
                }
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		//
		
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//9
		//
		
	}
	
}
