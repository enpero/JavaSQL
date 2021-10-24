package homework;

import org.apache.commons.lang3.RandomStringUtils;

import java.sql.*;
import java.util.Random;

public class Homework {
    private static final String CONNECTION_URL = "jdbc:mysql://localhost:3306/myDb";
    private static final String USER = "root";
    private static final String PASSWORD = "password";

    private static final String CREATE_CURATOR_SQL =
            "CREATE TABLE IF NOT EXISTS Curator(id int auto_increment primary key, fio varchar(50));";

    private static final String CREATE_GROUP_SQL =
            "CREATE TABLE IF NOT EXISTS Team(id int auto_increment primary key, " +
                    "name varchar(50), " +
                    "id_curator int, " +
                    "FOREIGN KEY(id_curator) REFERENCES Curator(id));";

    private static final String CREATE_STUDENT_SQL =
            "CREATE TABLE IF NOT EXISTS Student(id int auto_increment primary key, " +
                    "fio varchar(50), " +
                    "sex varchar(50), " +
                    "id_group int, " +
                    "FOREIGN KEY(id_group) REFERENCES Team(id));";

    private static final String INSERT_INTO_CURATOR =
            "INSERT IGNORE INTO Curator(fio) VALUES(?)";

    private static final String INSERT_INTO_GROUP =
            "INSERT IGNORE INTO Team(name, id_curator) VALUES(?, ?)";

    private static final String INSERT_INTO_STUDENT =
            "INSERT IGNORE INTO Student(fio,sex,id_group) VALUES(?,?,?)";

    private static final String SELECT_FROM_STUDENTS =
            "SELECT * FROM Student";

    private static final String GET_GROUP_WITH_CURATOR_SQL =
            "SELECT gr.id, gr.name, cr.fio FROM Team as gr JOIN Curator as cr ON gr.id_curator=cr.id";

    private static final String GET_COUNT_STUDENTS =
            "SELECT COUNT(*) FROM Student";

    private static final String GET_COUNT_STUDENTS_FEMALE =
            "SELECT COUNT(*) FROM Student WHERE sex = 'F'";

    private static final String UPDATE_THE_CURATOR_TEAM =
            "UPDATE Team set id_curator= 1 where name = 'IT'";

    public void createCuratorTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_CURATOR_SQL);
        }
    }

    public void createStudentTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_STUDENT_SQL);
        }
    }

    public void createGroupTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_GROUP_SQL);
        }
    }

    public void insertDataIntoGroupTable(Connection connection, String groupName, int curatorId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_GROUP)) {
            statement.setString(1, groupName);
            statement.setInt(2, curatorId);
            int insertRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number:" + insertRowsNumber);
        }
    }

    public String getRandomSex() {
        Random random = new Random();
        boolean isMale = random.nextBoolean();
        return isMale ? "M" : "F";
    }


    public void insertDataIntoCuratorTable(Connection connection, String fio) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_CURATOR)) {
            statement.setString(1, fio);
            int insertRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number:" + insertRowsNumber);
        }
    }


    public void insertDataIntoStudentTable(Connection connection, String fio, String getRandomSex, int id_groupp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_STUDENT)) {
            statement.setString(1, fio);
            statement.setString(2, getRandomSex);
            statement.setInt(3, id_groupp);
            int insertRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number:" + insertRowsNumber);
        }
    }

    public void printGroupWithCurator(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_GROUP_WITH_CURATOR_SQL)) {
            while (resultSet.next()) {
                int idGroup = resultSet.getInt(1);
                String name = resultSet.getString("name");
                String fio = resultSet.getString(3);
                String row = String.format("ID: %s, NAME: %s, FIO: %s", idGroup, name, fio);
                System.out.println(row);
            }
        }
    }

    public void printStudents(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(SELECT_FROM_STUDENTS)) {
            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String name = resultSet.getString(2);
                String sex = resultSet.getString(3);
                int id_group = resultSet.getInt(4);
                System.out.println("Student ID: " + id + "\nStudent Name: " + name + "\nStudent SEX: " + sex + "\nStudent Group: " + id_group + "\n\n");
            }
        }
    }

    public void printCountStudents(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_COUNT_STUDENTS)) {
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                System.out.println("Students: " + count);
            }
        }
    }

    public void printCountStudentsFemale(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_COUNT_STUDENTS_FEMALE)) {
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                System.out.println("Female students: " + count);
            }
        }
    }

    public void updateFioCurator(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            int updated = statement.executeUpdate(UPDATE_THE_CURATOR_TEAM);
            System.out.println("Table Curator was " + (updated == 0 ? "not " : "") + "updated" + "\n");
        }
    }

    public static void main(String[] args) {
        Homework homework = new Homework();

        try {
            try (Connection connection = DriverManager.getConnection(CONNECTION_URL, USER, PASSWORD)) {
                homework.createCuratorTable(connection);
                homework.createGroupTable(connection);
                homework.createStudentTable(connection);

                final String[] curators = new String[]{"Иванов", "Петров", "Сидоров", "Федоров"};
                for (String fio : curators) {
                    homework.insertDataIntoCuratorTable(connection, fio);
                }

                final String[] groups = new String[]{"IT", "ART", "SPORT"};
                int id = 1;
                for (String group : groups) {
                    homework.insertDataIntoGroupTable(connection, group, id);
                    id++;
                }

                for (int i = 0; i < 15; i++) {
                    Random random = new Random();
                    homework.insertDataIntoStudentTable
                            (connection, RandomStringUtils.randomAlphabetic(10), homework.getRandomSex(), random.nextInt(4 - 1) + 1);
                }
                homework.printStudents(connection);
                homework.printCountStudents(connection);
                homework.printCountStudentsFemale(connection);
                homework.updateFioCurator(connection);
                homework.printGroupWithCurator(connection);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

