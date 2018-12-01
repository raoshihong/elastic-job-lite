使用spring配置elastic-job时，使用了spring.handlers自定义命名空间和解析器
io.elasticjob.lite.spring.reg.parser.ZookeeperBeanDefinitionParser  这个是用来解析注册中心的配置的<reg:zookeeper>
io.elasticjob.lite.spring.job.handler.JobNamespaceHandler 绑定了三个解析器
