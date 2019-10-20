package com.github.hcsp;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.sql.*;
import java.util.stream.Collectors;

public class Main {
    @SuppressFBWarnings("DMI_CONSTANT_DB_PASSWORD")
    public static void main(String[] args) throws IOException, SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:file:/Users/sean/IdeaProjects/xiedaimala-crawler/news", "root", "123");
        String link;
        // 从数据库中加载下一个链接，如果能加载到，则进行循环
        while ((link = getNextLinkThenDelete(conn)) != null) {
            // 询问数据库，当前链接是不是已经被处理过了？
            if (isLinkProcessed(conn, link)) {
                continue;
            }
            if (isInterestingLink(link)) {
                System.out.println(link);
                Document doc = httpGetAndParseHtml(link);
                parseUrlsFromPageAndStoreIntoDatabase(conn, doc);
                // 假如这是一个新闻的详情页面，就存入数据库，否则什么也不做
                storeIntoDatabaseIfItIsNewsPage(conn, doc, link);
                updateDatabase(conn, link, "INSERT INTO LINKS_ALREADY_PROCESSED (LINK) VALUES (?)");
            }
        }
    }

    private static void parseUrlsFromPageAndStoreIntoDatabase(Connection conn, Document doc) throws SQLException {
        for (Element aTag : doc.select("a")) {
            String href = aTag.attr("href");
            if (href.startsWith("//")) {
                href = "https:" + href;
            }
            if (!href.toLowerCase().startsWith("javascript")) {
                updateDatabase(conn, href, "INSERT INTO LINKS_TO_BE_PROCESSED (LINK) VALUES (?)");
            }
        }
    }

    private static boolean isLinkProcessed(Connection conn, String link) throws SQLException {
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

    private static void updateDatabase(Connection conn, String href, String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, href);
            statement.executeUpdate();
        }
    }

    private static String getNextLink(Connection conn, String sql) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql); ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                return resultSet.getString(1);
            }
        }
        return null;
    }

    private static String getNextLinkThenDelete(Connection conn) throws SQLException {
        String link = getNextLink(conn, "select LINK from LINKS_TO_BE_PROCESSED LIMIT 1");
        if (link != null) {
            updateDatabase(conn, link, "DELETE FROM LINKS_TO_BE_PROCESSED WHERE LINK=?");
        }
        return link;
    }

    private static void storeIntoDatabaseIfItIsNewsPage(Connection conn, Document doc, String link) throws SQLException {
        Elements articleTags = doc.select("article");
        if (!articleTags.isEmpty()) {
            for (Element articleTag : articleTags) {
                String title = articleTag.child(0).text();
                String content = articleTag.select("p").stream()
                        .map(Element::text).collect(Collectors.joining("\n"));
                try (PreparedStatement statement = conn.prepareStatement("insert into NEWS (TITLE, CONTENT, URL, CREATED_AT, MODIFIED_AT) values (?,?,?,now(),now())")) {
                    statement.setString(1, title);
                    statement.setString(2, content);
                    statement.setString(3, link);
                    statement.executeUpdate();
                }
            }
        }
    }

    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(link);
        httpGet.setHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36");

        try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {
            HttpEntity entity1 = response1.getEntity();
            String html = EntityUtils.toString(entity1);
            return Jsoup.parse(html);
        }
    }

    // 这是我们感兴趣的，我们只处理新浪站内的链接
    private static boolean isInterestingLink(String link) {
        return isNewsPage(link) || isIndexPage(link) && isNotLoginPage(link);
    }

    private static boolean isNewsPage(String link) {
        return link.contains("news.sina.cn");
    }

    private static boolean isIndexPage(String link) {
        return "https://sina.cn".equals(link);
    }

    private static boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");
    }
}
