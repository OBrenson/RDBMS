package lab1.dataservice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class DataSource {

    private Connection connection;

    public DataSource(String propsPath) throws SQLException, IOException {
        connection = createConnection(propsPath);
    }

    public Map<ResultId, List<Object>> executeSql(String query) throws SQLException {
        List<ResultId> resultIds = parseQuery(query);
        if (resultIds == null) {
            throw new SQLException("Зарпос должен содержать наименования полей");
        }
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        Map<ResultId, List<Object>> resMap = new HashMap<>();
        for(ResultId resultId : resultIds) {
            resMap.put(resultId, new ArrayList<>());
        }
        while (rs.next()) {
            for (ResultId resultId : resultIds) {
               resMap.get(resultId).add(rs.getObject(resultId.getName()));
            }
        }
        return resMap;
    }

    private List<ResultId> parseQuery(String query) {
        if (query.contains("select")) {
            query = query.split("select")[1];
            query = query.split("from")[0];
            if (query.contains("*")) {
                return null;
            }
            String[] labels = query.split(",");
            List<ResultId> res = new ArrayList<>();
            int id = 0;
            for (String label : labels) {
                label = label.trim();
                if (label.contains(".")) {
                    label = label.split("\\.")[1];
                }
                res.add(new ResultId(label.trim(), id));
                id++;
            }
            return res;
        }
        return null;
    }

    private Connection createConnection(String propsPath) throws IOException, SQLException {
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(propsPath);
        properties.load(fis);
        fis.close();
        String drivers = properties.getProperty("jdbc.drivers");
        System.setProperty("jdbc.drivers", drivers);
        String url = properties.getProperty("jdbc.url");
        String pass = properties.getProperty("jdbc.password");
        String user = properties.getProperty("jdbc.username");
        return DriverManager.getConnection(url, user, pass);
    }
}
