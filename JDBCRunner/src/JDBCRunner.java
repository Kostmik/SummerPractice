import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";
    private static final String DATABASE_NAME = "catalogue_of_tickets";
    private static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    private static final String USER_NAME = "postgres";
    private static final String DATABASE_PASS = "postgres";

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            executeQueries(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            System.out.println("Соединение с базой данных успешно.");
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    public static void executeQueries(Connection connection) throws SQLException {
        System.out.println("Получение всех билетов, выпущенных после определенной даты");
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM main_catalogue WHERE date > '2006-01-01';")) {
            while (rs.next()) {
                printTicket(rs);
            }
        }

        System.out.println("\nПолучение всех билетов определенного типа");
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM main_catalogue WHERE type = 2;")) {
            while (rs.next()) {
                printTicket(rs);
            }
        }

        System.out.println("\nПолучение количества билетов каждого типа");
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT type, COUNT(*) AS count FROM main_catalogue GROUP BY type;")) {
            while (rs.next()) {
                System.out.println("Type: " + rs.getInt("type") + ", Count: " + rs.getInt("count"));
            }
        }

        System.out.println("\nПолучение всех уникальных комбинаций цветов бумаги и магнитных полос");
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT DISTINCT paper, stripe FROM main_catalogue;")) {
            while (rs.next()) {
                System.out.println("Paper: " + rs.getInt("paper") + ", Stripe: " + rs.getInt("stripe"));
            }
        }

        System.out.println("\nПолучение информации о билетах вместе с типами логотипов");
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT mc.id, mc.type, mc.description, lt.description AS logo_description " +
                     "FROM main_catalogue mc " +
                     "JOIN logo_type lt ON mc.logo = lt.id;")) {
            while (rs.next()) {
                System.out.println("ID: " + rs.getInt("id") + ", Type: " + rs.getInt("type") +
                        ", Description: " + rs.getString("description") +
                        ", Logo Description: " + rs.getString("logo_description"));
            }
        }

        System.out.println("\nПолучение информации о билетах вместе с типами бумаги");
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT mc.id, mc.type, mc.description, pc.description AS paper_description " +
                     "FROM main_catalogue mc " +
                     "JOIN paper_color pc ON mc.paper = pc.id;")) {
            while (rs.next()) {
                System.out.println("ID: " + rs.getInt("id") + ", Type: " + rs.getInt("type") +
                        ", Description: " + rs.getString("description") +
                        ", Paper Description: " + rs.getString("paper_description"));
            }
        }

        System.out.println("\nДобавление нового билета в таблицу");
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO main_catalogue (id, type, description, logo, paper, stripe, date, circulation, rarity, availability) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")) {
            statement.setInt(1, 196);
            statement.setInt(2, 1);
            statement.setString(3, "Новый билет");
            statement.setInt(4, 2);
            statement.setInt(5, 3);
            statement.setInt(6, 1);
            statement.setDate(7, Date.valueOf("2024-01-01"));
            statement.setInt(8, 100000);
            statement.setInt(9, 5);
            statement.setString(10, "+");

            int rowsInserted = statement.executeUpdate();
            if (rowsInserted > 0) {
                System.out.println("Новый билет добавлен успешно.");
            }
        }

        System.out.println("\nОбновление тиража билета по его ID");
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE main_catalogue SET circulation = 200000 WHERE id = 1;")) {
            int rowsUpdated = statement.executeUpdate();
            if (rowsUpdated > 0) {
                System.out.println("Тираж билета обновлен успешно.");
            }
        }

        System.out.println("\nУдаление билета по его ID");
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM main_catalogue WHERE id = 10;")) {
            int rowsDeleted = statement.executeUpdate();
            if (rowsDeleted > 0) {
                System.out.println("Билет удален успешно.");
            }
        }

        System.out.println("\nПолучение всех типов билетов, у которых средний тираж меньше 100000");
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT type, AVG(circulation) AS avg_circulation " +
                     "FROM main_catalogue " +
                     "GROUP BY type " +
                     "HAVING AVG(circulation) < 100000;")) {
            while (rs.next()) {
                System.out.println("Type: " + rs.getInt("type") + ", Avg Circulation: " + rs.getDouble("avg_circulation"));
            }
        }
    }

    private static void printTicket(ResultSet rs) throws SQLException {
        System.out.println("ID: " + rs.getInt("id") + ", Type: " + rs.getInt("type") +
                ", Description: " + rs.getString("description") + ", Logo: " + rs.getInt("logo") +
                ", Paper: " + rs.getInt("paper") + ", Stripe: " + rs.getInt("stripe") +
                ", Date: " + rs.getDate("date") + ", Circulation: " + rs.getInt("circulation") +
                ", Rarity: " + rs.getInt("rarity") + ", Availability: " + rs.getString("availability"));
    }
}