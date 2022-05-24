package cs223.group8.session;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import cs223.group8.mapper.DataItemMapper;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;

public class LeaderSessionConfig {
    public static SqlSession sqlSession = initSqlSessionFactory();

    public static SqlSession initSqlSessionFactory() {
        DataSource dataSource = dataSource();
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("Production", transactionFactory, dataSource);
        MybatisConfiguration configuration = new MybatisConfiguration(environment);
        configuration.addMapper(DataItemMapper.class);
        configuration.setLogImpl(StdOutImpl.class);
        return new MybatisSqlSessionFactoryBuilder().build(configuration).openSession(true);
    }

    public static DataSource dataSource() {
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass(org.postgresql.Driver.class);
        dataSource.setUrl("jdbc:postgresql://127.0.0.1:5432/leader");
        dataSource.setUsername("postgres");
        dataSource.setPassword("lyq990515");
//        try {
//            Connection connection = dataSource.getConnection();
//            Statement statement = connection.createStatement();
//            statement.execute("create table person (" +
//                    "id BIGINT(20) NOT NULL," +
//                    "name VARCHAR(30) NULL," +
//                    "age INT(11) NULL," +
//                    "PRIMARY KEY (id)" +
//                    ")");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        return dataSource;
    }

}
