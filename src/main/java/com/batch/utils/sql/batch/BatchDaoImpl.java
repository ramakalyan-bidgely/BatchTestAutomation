package com.batch.utils.sql.batch;/*
package com.batch.utils.sql.batch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import com.batch.utils.sql.batch.BatchDetails;
import com.batch.utils.sql.batch.BatchStepDetails;
public class BatchDaoImpl{

    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost/ca";
    private static final String ID = "root";
    private static final String PASS = "";


    private static final String FIND_ALL = "SELECT * FROM user ORDER BY id";
    private static final String FIND_BY_ID = "SELECT * FROM user WHERE id=?";
    private static final String FIND_BY_NAME = "SELECT * FROM user WHERE name=?";

    public List<User> findAll() {
        Connection conn = null;
        PreparedStatement stmt = null;
        List<User> list = new ArrayList<User>();

        try {
            conn = getConnection();
            stmt = conn.prepareStatement(FIND_ALL);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                User user = new User();
                user.setId(rs.getInt("id"));
                user.setName(rs.getString("name"));
                user.setTel(rs.getString("tel"));
                user.setPasswd(rs.getString("passwd"));

                list.add(user);
            }
        } catch (SQLException e) {
            // e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            close(stmt);
            close(conn);
        }

        return list;
    }

    public User findById(int id) {
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = getConnection();
            stmt = conn.prepareStatement(FIND_BY_ID);
            stmt.setInt(1, id);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                User user = new User();
                user.setId(rs.getInt("id"));
                user.setName(rs.getString("name"));
                user.setTel(rs.getString("tel"));
                user.setPasswd(rs.getString("passwd"));

                return user;
            } else {
                return null;
            }
        } catch (SQLException e) {
            // e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            close(stmt);
            close(conn);
        }
    }

    public User findByName(String name) {
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = getConnection();
            stmt = conn.prepareStatement(FIND_BY_NAME);
            stmt.setString(1, name);

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                User user = new User();
                user.setId(rs.getInt("id"));
                user.setName(rs.getString("name"));
                user.setTel(rs.getString("tel"));
                user.setPasswd(rs.getString("passwd"));

                return user;
            } else {
                return null;
            }
        } catch (SQLException e) {
            // e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            close(stmt);
            close(conn);
        }
    }


    public int update(User user) {
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = getConnection();
            stmt = conn.prepareStatement(UPDATE);
            stmt.setString(1, user.getName());
            stmt.setString(2, user.getTel());
            stmt.setString(3, user.getPasswd());
            stmt.setInt(4, user.getId());

            return stmt.executeUpdate();
        } catch (SQLException e) {
            // e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            close(stmt);
            close(conn);
        }
    }

    private Connection getConnection() {
        try {
            Class.forName(DRIVER_NAME);
            return DriverManager.getConnection(DB_URL, ID, PASS);
        } catch (Exception e) {
            // e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void close(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                // e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }


    private static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                // e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

}
*/
