package cs223.group8.session;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cs223.group8.mapper.DataItemMapper;
import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class GeneralSessionConfig {
    public static Config jdbcConfig = ConfigFactory.parseFile(Paths.get("jdbc.conf").toAbsolutePath().toFile());
    public static String jdbcUrl = jdbcConfig.getString("jdbc.url");
    public static String jdbcUserName = jdbcConfig.getString("jdbc.username");
    public static String jdbcPassword = jdbcConfig.getString("jdbc.password");

    public static String currentDBName = "leader";

    public static SqlSession sqlSession = initSqlSessionFactory("leader");

    public static SqlSession initSqlSessionFactory(String dbName) {
        DataSource dataSource = dataSource(dbName);
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("Production", transactionFactory, dataSource);
        MybatisConfiguration configuration = new MybatisConfiguration(environment);
        configuration.setLogImpl(NoLoggingImpl.class);
        configuration.addMapper(DataItemMapper.class);
        return new MybatisSqlSessionFactoryBuilder().build(configuration).openSession(true);
    }

    public static void changeSession(String dbName) {
        sqlSession = initSqlSessionFactory(dbName);
        currentDBName = dbName;
    }

    public static void createNewSession(String dbName) {
        DataSource dataSource = createNewDataSource(dbName);
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("Production", transactionFactory, dataSource);
        MybatisConfiguration configuration = new MybatisConfiguration(environment);
        configuration.setLogImpl(NoLoggingImpl.class);
        configuration.addMapper(DataItemMapper.class);
        sqlSession = new MybatisSqlSessionFactoryBuilder().build(configuration).openSession(true);
    }

    public static DataSource createNewDataSource(String dbName) {
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass(org.postgresql.Driver.class);
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(jdbcUserName);
        dataSource.setPassword(jdbcPassword);
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("create database " + dbName);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

        }
        dataSource.setUrl(jdbcUrl + dbName);
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("create table data_item (" +
                    "key   varchar(100) constraint data_item_pk primary key," +
                    "value int not null" +
                    ")");
        } catch (SQLException e){
            e.printStackTrace();
        }
        return dataSource;
    }

    public static DataSource dataSource(String dbName) {
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass(org.postgresql.Driver.class);
        dataSource.setUrl(jdbcUrl + dbName);
        dataSource.setUsername(jdbcUserName);
        dataSource.setPassword(jdbcPassword);
        return dataSource;
    }
}
