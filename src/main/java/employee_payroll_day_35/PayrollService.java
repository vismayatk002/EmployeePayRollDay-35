package employee_payroll_day_35;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Statement;

public class PayrollService {
	
	public void insertData(Connection con) throws SQLException {
		
		//Insert a data
		try {
			if(con != null) {
				con.setAutoCommit(false);
				
				String insertQuery = "INSERT INTO Employee(emp_name,gender,address,phone_no,start_date) VALUES (?,?,?,?,?)";
				PreparedStatement insertStatement = con.prepareStatement(insertQuery);
				insertStatement.setString(1, "Kalyani");
				insertStatement.setString(2, "F");
				insertStatement.setString(3, "Hyderabad");
				insertStatement.setString(4, "9870675432");
				insertStatement.setDate(5, new Date(2021-07-13));
				int rowInserted = insertStatement.executeUpdate();
				
				
				insertQuery = "INSERT INTO employee_payroll(emp_id,basic_pay,deduction,taxable_pay,tax,net_pay ) VALUES (?,?,?,?,?,?)";
				insertStatement = con.prepareStatement(insertQuery);
				insertStatement.setInt(1, 5);
				insertStatement.setDouble(2, 3500000);
				insertStatement.setDouble(3, 700000);
				insertStatement.setDouble(4, 2800000);
				insertStatement.setDouble(5, 280000);
				insertStatement.setDouble(6, 3220000);
				rowInserted = insertStatement.executeUpdate();
				
				con.commit();
				if(rowInserted > 0){
					System.out.println("Data Inserted");
				}
				con.setAutoCommit(true);
			}
			
		}catch(SQLException sqlException) {
			System.out.println("Insertion Rollbacked");
			con.rollback();

	    }finally {
	    	if(con != null) {
				con.close();
	    	}
	    }
	}
	
	public void readAllData(Connection con) throws SQLException {
		
		//Retrieve all the data
		if(con != null) {
		   String retrieveQuery = "SELECT * FROM Employee";
		   Statement statement = (Statement) con.createStatement();
		   ResultSet resultSet = statement.executeQuery(retrieveQuery);
		   
		   while(resultSet.next()) {
			   
			   int emp_id = resultSet.getInt("emp_id");
			   String emp_name = resultSet.getString("emp_name");
			   String gender = resultSet.getString("gender");
			   String address = resultSet.getString("address");
			   String phone_no = resultSet.getString("phone_no");
			   String start_date = resultSet.getDate("start_date").toString();
			   
			   String rowData = String.format("\nEmp_id : %d \nEmp_name : %s \nGender : %s \nAddress : %s \nPhone_no : %s \nStartDate : %s",emp_id,emp_name, gender, address,phone_no,start_date);
			   System.out.println("Data retrieved " + rowData);
		   }
		   
		   retrieveQuery = "SELECT * FROM employee_payroll";
		   statement = (Statement) con.createStatement();
		   resultSet = statement.executeQuery(retrieveQuery);
		   
		   while(resultSet.next()) {
			   
			   int payroll_id = resultSet.getInt("payroll_id");
			   int emp_id = resultSet.getInt("emp_id");
			   Double basic_pay = resultSet.getDouble("basic_pay");
			   Double deduction = resultSet.getDouble("deduction");
			   Double taxable_pay = resultSet.getDouble("taxable_pay");
			   Double tax = resultSet.getDouble("tax");
			   Double net_pay = resultSet.getDouble("net_pay");
			   
			   String rowData = String.format("\nPayroll_id : %d \nEmp_id : %d \nBasic_pay : %f\nDeduction : %f \nTaxable_pay : %f \nTax : %f \nNet_pay : %f",payroll_id,emp_id,basic_pay,deduction,taxable_pay,tax,net_pay);
			   System.out.println("Data retrieved " + rowData);
		   }
		}
	}
	private static Connection getSqlConnection() {
		Connection con = null;
	       try {
	    	   
	    	   String dbHostUrl = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
	    	   String userName = "root";
	    	   String passWord = "vismaya";
	    	   
	    	   con = DriverManager.getConnection(dbHostUrl, userName, passWord);
	    	   
	    	   if(con != null) {
	    		   System.out.println("Connection is Established");
	    	   }
	    	   
	       }catch(SQLException sqlException) {
	    	   System.out.println(sqlException.getMessage());
	       }
		return con;
	}
    public static void main( String[] args ){
    	
    	PayrollService payroll = new PayrollService();
		Connection con = getSqlConnection();
		
		try {
			payroll.insertData(con);
			payroll.readAllData(con);
			
		}catch(SQLException sqlException) {
	    	   System.out.println(sqlException.getMessage());
	       }finally {
	    	   
	    	   if(con != null) {
	    		   try {
	    			   con.close();
	    		   }catch(SQLException sqlException) {
	    	    	   System.out.println(sqlException.getMessage());
	    		   }
	    	   }
	       }
    }
}
