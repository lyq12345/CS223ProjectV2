package cs223.group8.session;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import cs223.group8.mapper.DataItemMapper;
import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class GeneralSessionConfig {
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
        dataSource.setUrl("jdbc:postgresql://127.0.0.1:5432/");
        dataSource.setUsername("postgres");
        dataSource.setPassword("lyq990515");
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("create database " + dbName);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

        }
        dataSource.setUrl("jdbc:postgresql://127.0.0.1:5432/" + dbName);
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
        dataSource.setUrl("jdbc:postgresql://127.0.0.1:5432/" + dbName);
        dataSource.setUsername("postgres");
        dataSource.setPassword("lyq990515");
        return dataSource;
    }
}
