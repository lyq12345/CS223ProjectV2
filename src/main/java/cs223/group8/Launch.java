package cs223.group8;


public class Launch {


    public static void main(String[] args) {
//        SqlSessionFactory sqlSessionFactory = LeaderSessionFactory.initSqlSessionFactory();
//        try (SqlSession session = sqlSessionFactory.openSession(true)) {
//            DataItemMapper mapper = session.getMapper(DataItemMapper.class);
//            QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
//            queryWrapper.eq("key", "z");
//            DataItem item = mapper.selectOne(queryWrapper);
//            System.out.println(item.getValue());
//        }

        WorkflowController workflowController = new WorkflowController();
        workflowController.load("src/main/java/cs223/group8/readonly.txt");
        workflowController.run();
    }
}