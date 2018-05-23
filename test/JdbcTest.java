package test;

import template.JdbcInserter;
import template.JdbcSelector;

import java.sql.*;
import java.util.UUID;

public class JdbcTest {

    /* Database config */
    private static final String dbUrl = "jdbc:mysql://localhost:3306/testdb";
    private static final String user = "swj";
    private static final String password = "123456";
    private static final String tableName = "test";

    public static void main(String[] args) {
        //singleThreadInsertTest(100000);
        singleThreadSelectTest();
        //delete(10000);
        //multiThreadInsertTest(1000000,4);
        //multiThreadSelectTest(64,1000000);
    }

    private static void delete(long total) {
        JdbcInserter jdbcInserter = new JdbcInserter(dbUrl, user, password);
        jdbcInserter.delete(total);
    }

    private static void singleThreadInsertTest(long total) {
        JdbcInserter jdbcInserter = new JdbcInserter(dbUrl, user, password);
        jdbcInserter.singleThreadInsert(total);
    }

    private static void multiThreadInsertTest(long total, int processors) {
        //int processors = Runtime.getRuntime().availableProcessors();
        System.out.println("------ Processors: " + processors + " ------");

        JdbcInserter jdbcInserter = new JdbcInserter(dbUrl, user, password);
        jdbcInserter.multiThreadInsert(total, processors);
    }

    private static void singleThreadSelectTest() {

        /* Select config */
        String query = null;

        JdbcSelector jdbcSelector = new JdbcSelector(dbUrl, user, password);
        jdbcSelector.singleThreadSelect(query);
    }

    private static void multiThreadSelectTest(int threads, long total) {
        JdbcSelector jdbcSelector = new JdbcSelector(dbUrl, user, password);
        jdbcSelector.multiThreadSelect(threads,total);
    }

}
