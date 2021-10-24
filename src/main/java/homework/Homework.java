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
            "CREATE TABLE IF NOT EXISTS Groupp(id int auto_increment primary key, " +
                    "name varchar(50), " +
                    "id_curator int, " +
                    "FOREIGN KEY(id_curator) REFERENCES Curator(id));";

    private static final String CREATE_STUDENT_SQL =
            "CREATE TABLE IF NOT EXISTS Student(id int auto_increment primary key, " +
                    "fio varchar(50), " +
                    "sex varchar(50), " +
                    "id_group int, " +
                    "FOREIGN KEY(id_group) REFERENCES Groupp(id));";

    private static final String INSERT_INTO_CURATOR =
            "INSERT INTO Curator(fio) VALUES(?), (?), (?), (?)";

    private static final String INSERT_INTO_GROUP =
            "INSERT INTO Groupp(name, id_curator) VALUES(?), (?), (?)";

    private static final String INSERT_INTO_STUDENT =
            "INSERT INTO Student(fio,sex,groupp) VALUES(?,?,?)";

    private static final String SELECT_FROM_STUDENTS =
            "SELECT * FROM Student";

    private static final String GET_GROUP_WITH_CURATOR_SQL =
            "SELECT gr.id, gr.name, cr.fio FROM Groupp as gr JOIN Curator as cr ON gr.id_curator=cr.id";

    private static final String GET_COUNT_STUDENTS =
            "SELECT COUNT(*) FROM Student";

    private static final String GET_COUNT_STUDENTS_FEMALE =
            "SELECT COUNT(*) FROM Student WHERE sex = 'F'";

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

    public void insertDataIntoCuratorTable(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_CURATOR)) {
            statement.setString(1, "Ivanov");
            statement.setString(2, "Petrov");
            statement.setString(3, "Sidorov");
            statement.setString(4, "Fedorov");
            int insertRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number:" + insertRowsNumber);
        }
    }

    public void insertDataIntoGroupTable(Connection connection, int id_curator) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_GROUP)) {
            statement.setInt(id_curator, 1);
            statement.setInt(id_curator, 2);
            statement.setInt(id_curator, 3);
            statement.setString(1, "IT");
            statement.setString(2, "ART");
            statement.setString(3, "Sport");
            int insertRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number:" + insertRowsNumber);
        }
    }

    public String getRandomSex() {
        Random random = new Random();
        boolean isMale = random.nextBoolean();
        return isMale ? "M" : "F";
    }

    public void insertDataIntoStudentTable(Connection connection, String fio, String getRandomSex, String groupp) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_INTO_STUDENT)) {
            statement.setString(1, fio);
            statement.setString(2, getRandomSex);
            statement.setString(3, groupp);
            int insertRowsNumber = statement.executeUpdate();
            System.out.println("Inserted rows number:" + insertRowsNumber);
        }
    }

    public void printGroupWithCurator(Connection connection) throws SQLException {
        try(Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(GET_GROUP_WITH_CURATOR_SQL)) {
                while(resultSet.next()) {
                    int idGroup = resultSet.getInt(1);
                    String name = resultSet.getString("name");
                    String fio = resultSet.getString(3);
                    String row = String.format("ID: %s, NAME: %s, FIO: %s", idGroup, name, fio);
                    System.out.println(row);
                }
            }
        }

    public void printStudents(Connection connection) throws SQLException {
        try(Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SELECT_FROM_STUDENTS)) {
            while(resultSet.next()) {
                int id = resultSet.getInt(1);
                String name = resultSet.getString(2);
                String sex = resultSet.getString(3);
                int id_group = resultSet.getInt(4);
                System.out.println("Student ID: " + id + "\nStudent Name: " + name + "\nStudent SEX: " + sex + "\nStudent Group: " + id_group + "\n\n");
                }
            }
        }

    public void printCountStudents(Connection connection) throws SQLException {
        try(Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(GET_COUNT_STUDENTS)) {
            while(resultSet.next()) {
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

    public static void main(String[] args) {
        Homework homework = new Homework();

        try {
            try (Connection connection = DriverManager.getConnection(CONNECTION_URL, USER, PASSWORD)) {
                homework.createCuratorTable(connection);
                homework.createGroupTable(connection);
                homework.createStudentTable(connection);
                homework.insertDataIntoCuratorTable(connection);
                homework.insertDataIntoGroupTable(connection, 3);
                for (int i = 0; i < 15; i++) {
                    homework.insertDataIntoStudentTable
                            (connection, RandomStringUtils.randomAlphabetic(10), homework.getRandomSex(), RandomStringUtils.random());
                }
                homework.printGroupWithCurator(connection);
                homework.printStudents(connection);
                homework.printCountStudents(connection);
                homework.printCountStudentsFemale(connection);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

