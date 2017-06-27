package dao;
import java.sql.Connection;
import java.sql.DriverManager;
import javax.swing.JOptionPane;

public class connection {
 
	// 数据库驱动
	private static final String driver = "com.mysql.jdbc.Driver";
	private static String url = "jdbc:mysql://localhost:3306/real_data";// 数据库URL
	private static Connection conn = null;// 数据库连接
	private static connection dao = null;
	
	public connection() {
		try {
			//System.out.println("尝试加载");
			Class.forName(driver);
		//	System.out.println("加载驱动已经成功");
			conn = DriverManager.getConnection(url,"root","lxsjw495");
			// 连接数据库成功
		} 
		catch (Exception e)
		{
			e.printStackTrace();
			JOptionPane.showMessageDialog(null, "数据库连接异常，或者本软件已经运行。");
			System.exit(0);
		}
	}
	
	public static Connection getDao() {// 获取conn实例
		if (dao == null)
			dao = new connection();
		return conn;
	}
}
