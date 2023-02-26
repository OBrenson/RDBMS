package lab1;

import lab1.dataservice.DataSource;
import lab1.dataservice.ResultId;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Main {

    private static String PROPS_PATH = "C:\\Users\\PC\\Desktop\\labs\\RDBMS\\lab1-pqsql\\database.properties";

    public static void main(String[] args) throws IOException {

        DataSource ds = null;
        try {
            ds = new DataSource(PROPS_PATH);
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }

        String query = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while(!"exit".equals(query)) {
            query = reader.readLine();
            Map<ResultId, List<Object>> resMap = null;
            try {
                resMap = ds.executeSql(query.toLowerCase());
            } catch (SQLException e) {
                e.printStackTrace();
                continue;
            }
            for (Map.Entry<ResultId, List<Object>> entry : resMap.entrySet()) {
                System.out.println(entry.getKey().getName());
                int id = 0;
                for(Object obj : entry.getValue()) {
                    System.out.println(id + " " + obj.toString());
                }
            }
        }
    }

}
