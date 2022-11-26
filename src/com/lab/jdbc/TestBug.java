package com.lab.jdbc;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class TestBug {

    public static final String ROOM_PATH = "res/room.csv";
    public static final String STUDENT_PATH = "res/student.csv";

    public static final String ROOM_EXP_PATH = "res/room_exp.csv";
    public static final String STUDENT_EXP_PATH = "res/student_exp.csv";

    public static final String ROOM_SQL = "sql/room.sql";
    public static final String STUDENT_SQL = "sql/student.sql";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        Class.forName("com.mysql.cj.jdbc.Driver");

        System.out.println("Add independent print");

        Scanner objIn = new Scanner(System.in);
        System.out.println("Enter url following the prescribed format. e.g. \njdbc:mySql://localhost:3306/lab1");
        String url =  objIn.nextLine();
        System.out.println("Enter database account following the prescribed format. e.g. \nroot");
        String account = objIn.nextLine();
        System.out.println("Enter database password following the prescribed format. e.g. \n12345678");
        String password = objIn.nextLine();

        Connection conn;
        try {
            conn = DriverManager.getConnection(url, account, password);
        } catch (Exception e) {
            System.out.println("connect failed");
            return;
        }

        Statement statement = conn.createStatement();

        //create table
        String r_sql_init = new BufferedReader(new FileReader(new File(ROOM_SQL))).readLine();
        statement.execute(r_sql_init);

        String s_sql_init = new BufferedReader(new FileReader(new File(STUDENT_SQL))).readLine();
        statement.execute(s_sql_init);

        //read room.csv
        CsvReader r_reader;
        try {
            r_reader = new CsvReader(ROOM_PATH,',',Charset.forName("GBK"));
        } catch (Exception e) {
            System.out.println("room.csv failed");
            return;
        }
        r_reader.readHeaders();
        int r_len = r_reader.getHeaders().length;
        String r_sql_insert;
        String r_attributes = new BufferedReader(new FileReader(new File(ROOM_PATH))).readLine().replaceAll("\"", "");

        CsvWriter r_writer = new CsvWriter(ROOM_EXP_PATH, ',', Charset.forName("GBK"));
        r_writer.writeRecord(r_reader.getHeaders());

        int r_order = 0;
        while (r_reader.readRecord()) {
            ++r_order;

            r_sql_insert = "INSERT INTO room ("
                    + r_attributes
                    + ")"
                    + "VALUES('"
                    + r_reader.get(0).replaceAll("\"", "")
                    + "'";

            for (int i = 1; i < r_len; i++) {
                r_sql_insert += ","
                        + "'"
                        + r_reader.get(i).replaceAll("\"", "")
                        + "'";
            }

            r_sql_insert += ");";

            //insert && handle special values
            try {
                statement.execute(r_sql_insert);
            } catch (Exception e) {
                System.out.println("room-line " + r_order + ": " + e);
                r_writer.writeRecord(r_reader.getValues());
            }
        }
        System.out.println("room.csv finish");

        r_reader.close();
        r_writer.close();



        //read student.csv
        CsvReader s_reader;
        try {
            s_reader = new CsvReader(STUDENT_PATH,',', StandardCharsets.UTF_8);
        } catch (Exception e) {
            System.out.println("student.csv failed");
            return;
        }
        s_reader.readHeaders();
        int s_len = s_reader.getHeaders().length;
        String s_sql_insert;
        String s_attributes = new BufferedReader(new FileReader(new File(STUDENT_PATH))).readLine().replaceAll("\"", "");

        CsvWriter s_writer = new CsvWriter(STUDENT_EXP_PATH,',', StandardCharsets.UTF_8);
        s_writer.writeRecord(s_reader.getHeaders());

        int s_order = 0;
        while (s_reader.readRecord()) {
            ++s_order;

            s_sql_insert = "INSERT INTO student ("
                    + s_attributes
                    + ")"
                    + "VALUES('"
                    + s_reader.get(0).replaceAll("\"", "")
                    + "'";

            for (int i = 1; i < s_len; i++) {
                s_sql_insert += ","
                        + "'"
                        + s_reader.get(i).replaceAll("\"", "")
                        + "'";
            }
            s_sql_insert += ");";

            //insert && handle special values
            try {
                statement.execute(s_sql_insert);
            } catch (Exception e) {
                System.out.println("student-line " + s_order + ": " + e);
                s_writer.writeRecord(s_reader.getValues());
            }
        }
        System.out.println("student.csv finish");

        s_reader.close();
        s_writer.close();


        statement.close();
        conn.close();
    }
}
