package com.github.hcsp;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.*;

public class JdbcCrawlerDao implements CrawlerDao {

    private final Connection conn;

    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public JdbcCrawlerDao() {
        try {
            this.conn = DriverManager.getConnection("jdbc:h2:file:/Users/sean/IdeaProjects/xiedaimala-crawler/news", "root", "123");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getNextLink(String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql); ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }

    public String getNextLinkThenDelete() throws SQLException {
        String link = getNextLink("select LINK from LINKS_TO_BE_PROCESSED LIMIT 1");
        if (link != null) {
            updateDatabase(link, "DELETE FROM LINKS_TO_BE_PROCESSED WHERE LINK=?");
        }
        return link;
    }

    public void updateDatabase(String href, String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, href);
            statement.executeUpdate();
        }
    }

    public void insertNewsIntoDatabase(String url, String title, String content) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement("insert into NEWS (TITLE, CONTENT, URL, CREATED_AT, MODIFIED_AT) values (?,?,?,now(),now())")) {
            statement.setString(1, title);
            statement.setString(2, content);
            statement.setString(3, url);
            statement.executeUpdate();
        }
    }

    public boolean isLinkProcessed(String link) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = conn.prepareStatement("SELECT LINK FROM LINKS_ALREADY_PROCESSED WHERE LINK=?")) {
            statement.setString(1, link);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return false;
    }

    @Override
    public void insertProcessedLink(String link) {

    }

    @Override
    public void insertLinkToBeProcessed(String href) {

    }
}
