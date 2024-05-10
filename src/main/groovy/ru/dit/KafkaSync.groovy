package ru.dit

import groovy.yaml.YamlSlurper
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.clients.admin.AlterConfigOp
//@Grab('com.aestasit.infrastructure.sshoogr:sshoogr:0.9.28')
import static com.aestasit.infrastructure.ssh.DefaultSsh.*
import com.aestasit.infrastructure.ssh.SshOptions
import com.aestasit.infrastructure.ssh.dsl.SshDslEngine
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.clients.admin.ConfigEntry
import groovy.ginq.transform.GQ
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.CreatePartitionsOptions
import org.apache.kafka.clients.admin.CreateAclsOptions

SEP = '|'
FIELD_COUNT=5
GENERAL_TOPICS_CONF="configuration/generalTopics.conf"
REPLICATION_FACTOR=3
CONFIG_CACHE=[:]
PASSWORD_LENGTH=10
JAAS_CONF_NAME="kafka_server_jaas.conf"
CLUSTERS_WITH_SIMPLE_PASSWORD=["moscow_dev","moscow_test"]

def getKafkaAdminClient(String cluster){
    var yamlFile = new File("clusters.yaml")
    var yaml = new YamlSlurper().parse(yamlFile)
    //println(yaml)
    props = new HashMap<String,Object>()
    props["bootstrap.servers"]=yaml."$cluster".bootstrapServer
    props["security.protocol"]=yaml."$cluster".securityProtocol
    props["sasl.mechanism"]=yaml."$cluster".saslMechanism
    props["sasl.jaas.config"]="""org.apache.kafka.common.security.plain.PlainLoginModule required username="${yaml."$cluster".user}" password="${yaml."$cluster".password}";""".toString()
    return AdminClient.create(props)

}


def getConfigFromCsvFile(String clusterName){
    if(CONFIG_CACHE[clusterName]) { return CONFIG_CACHE[clusterName] }
    var configFile = new File("configuration/${clusterName}_topics.conf")
    var generalConfigFile = new File(GENERAL_TOPICS_CONF)
    var config = configFile.readLines().grep { it.contains(SEP) && !it.matches(/\s*#.*/)}.collect {it.tokenize(SEP)}
    var generalConfig = generalConfigFile.readLines().grep { it.contains(SEP) && !it.matches(/\s*#.*/)}.collect {it.tokenize(SEP)}
    config+=generalConfig
    if(!config.every {it.size() == FIELD_COUNT}) throw new Exception("Файл ${clusterName}_topics.conf или ${GENERAL_TOPICS_CONF} содержит неудовлетворительное количество полей по разделителю $SEP, должно быть $FIELD_COUNT")
    if(config.collect {it[0].strip()}.unique().size()<config.size()) throw new Exception("Файл ${clusterName}_topics.conf или ${GENERAL_TOPICS_CONF} содержит более одной строки с одним и тем же топиком")
    CONFIG_CACHE[clusterName]=config
    return config
}

def getAclsFromConfig(config){
    var acls=[]
    for(item in config){
        var topic = item[0].strip()

        //права на чтение
        for(user_with_group in item[1].split(',')){
            (user,group)=user_with_group.split('@')
            user="User:"+user.strip()
            group=group.strip()
            acls.add(new AclBinding(new ResourcePattern(ResourceType.TOPIC,topic,PatternType.LITERAL),new AccessControlEntry(user,"*",AclOperation.READ,AclPermissionType.ALLOW)))
            acls.add(new AclBinding(new ResourcePattern(ResourceType.TOPIC,topic,PatternType.LITERAL),new AccessControlEntry(user,"*",AclOperation.DESCRIBE,AclPermissionType.ALLOW)))
            acls.add(new AclBinding(new ResourcePattern(ResourceType.GROUP,group,PatternType.LITERAL),new AccessControlEntry(user,"*",AclOperation.READ,AclPermissionType.ALLOW)))
        }
        //права на запись
        for(user in item[2].split(',')){
            //println user
            user="User:"+user.strip()
            acls.add(new AclBinding(new ResourcePattern(ResourceType.TOPIC,topic,PatternType.LITERAL),new AccessControlEntry(user,"*",AclOperation.WRITE,AclPermissionType.ALLOW)))

        }
    }
    return acls.unique()
}

def getTopicsFromConfig(conf){
    conf.collect {it[0]}
}

def getPartitionsCountFromConf(cluster,topic){
   return getConfigFromCsvFile(cluster).find {it[0] == topic}[4] as Integer

}

def createMissingTopics(cluster){
    var yamlFile = new File("clusters.yaml")
    var yaml = new YamlSlurper().parse(yamlFile)
    var kafkaAdminClient = getKafkaAdminClient(cluster)
    var configTopics = getTopicsFromConfig(getConfigFromCsvFile(cluster))
    var kafkaTopics = kafkaAdminClient.listTopics().names().get()
    var missingTopics=configTopics - kafkaTopics
    kafkaAdminClient.createTopics(missingTopics.collect { new NewTopic(it,getPartitionsCountFromConf(cluster,it),(short)yaml."$cluster".replicationFactor.toInteger())}).all().get()
    kafkaAdminClient.close()
}

def generatePassword(){

    var symbols="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    var len=symbols.length()
    var str=""
    var random = new Random()
    for(i=1;i<=PASSWORD_LENGTH;i++ ){

        str+=symbols[random.nextInt(len)]
    }
    return str

}

def getFileContentFromKafka(cluster,fileName){
    var yamlFile = new File("clusters.yaml")
    var yaml = new YamlSlurper().parse(yamlFile)
    var home = yaml."$cluster".home
    var kafkaServer = yaml."$cluster".bootstrapServer[0].split(":")[0]

    options = new SshOptions()
    options.with {
        // logger = new SysOutLogger()
        defaultHost = kafkaServer
        defaultUser = yaml."$cluster".sshUser
        if(yaml."$cluster".sshPassword) {
            defaultPassword = yaml."$cluster".sshPassword
        }
        if(yaml."$cluster".sshKeyPath) {
            defaultKeyFile = new File(yaml."$cluster".sshKeyPath)
        }
        defaultPort = 22
        // reuseConnection = true
        trustUnknownHosts = true
    }

    def engine = new SshDslEngine(options)
    engine.remoteSession {

        return remoteFile("${home}/config/${fileName}").text
    }
}
def saveFileContentToKafka(cluster, fileName, content){
    var yamlFile = new File("clusters.yaml")
    var yaml = new YamlSlurper().parse(yamlFile)
    var home = yaml."$cluster".home
    kafkaServers = yaml."$cluster".bootstrapServer.collect { it.split(":")[0] }

    kafkaServers.each { server ->
        options = new SshOptions()
        options.with {
            // logger = new SysOutLogger()
            defaultHost = server
            defaultUser = yaml."$cluster".sshUser
            if(yaml."$cluster".sshPassword) {
                defaultPassword = yaml."$cluster".sshPassword
            }
            if(yaml."$cluster".sshKeyPath) {
                defaultKeyFile = new File(yaml."$cluster".sshKeyPath)
            }
            defaultPort = 22
            // reuseConnection = true
            trustUnknownHosts = true
        }

        def engine = new SshDslEngine(options)
        engine.remoteSession {
            //  exec 'touch /tmp/Qwerty'
            remoteFile("/tmp/${fileName}").text = content
            prefix("sudo") {
                exec "mkdir -p ${home}/config/backup"
                exec "cp ${home}/config/${JAAS_CONF_NAME} ${home}/config/backup/${JAAS_CONF_NAME}_${new Date().hashCode()}"
                exec "cp /tmp/${JAAS_CONF_NAME} ${home}/config/"
                exec "systemctl restart kafka"

            }
        }
        sleep(15)


    }
}


def getUsersFromKafkaConfig(cluster){
    var jaasFileContent = getFileContentFromKafka(cluster,JAAS_CONF_NAME)

    def kafkaUsers=jaasFileContent.split('\n').collect {
        def matcher = it =~ "user_(.+)="
        if (matcher) {
            matcher.group(1)
        }
    }
    kafkaUsers.removeAll {it==null}
    return kafkaUsers
}

def getUsersFromCsvConfig(cluster){
    var conf = getConfigFromCsvFile(cluster)
    users=[]
    for(item in conf) {
        users.addAll item[1].split(",").collect { it.split("@")[0].strip() }
        users.addAll item[2].split(",")*.strip()
    }
    return users.unique()
}

def addMissingUsers(cluster){
    var usersForCreation = getUsersFromCsvConfig(cluster) - getUsersFromKafkaConfig(cluster)
    if(!usersForCreation) return
    var currentJaasConf = getFileContentFromKafka(cluster,JAAS_CONF_NAME)
    var users
    if(cluster in CLUSTERS_WITH_SIMPLE_PASSWORD){
        users = String.join("\n", usersForCreation.collect { "user_" + it + "=\"" + it + "\"" })
    } else {
        users = String.join("\n", usersForCreation.collect { "user_" + it + "=\"" + generatePassword() + "\"" })
    }
    var matcher = currentJaasConf =~ /([\s\S]*);\s+};/
    var newJaasConf
    if(matcher){
        newJaasConf="${matcher.group(1)}\n${users};\n};"
    }
    saveFileContentToKafka(cluster,JAAS_CONF_NAME,newJaasConf)
    //println newJaasConf
}


def updateTopicsConfiguration(cluster){
    var kafkaAdminClient = getKafkaAdminClient(cluster)
    var conf = getConfigFromCsvFile(cluster)
    var configResourceList = getTopicsFromConfig(conf).collect { topic -> new ConfigResource(ConfigResource.Type.TOPIC,topic) }
    var configEntryMapKafka = kafkaAdminClient.describeConfigs(configResourceList).all().get()
    var alterConfigOpMap = [:]
    println configEntryMapKafka
    conf.each {
        var topic = it[0]
        var configStr = it[3]
        var alterConfigOpList = []
        var configEntryes=[]
        configStr.split(",").each {
            def (paramName, paramValue) = it.split("=")*.strip()
            configEntryes << new ConfigEntry(paramName, paramValue)
        }
        // ниже не работает, та как есть доп поля, содержимое которых отличаются от дефолтных значений
       // var diffConfigEntryes = configEntryes - configEntryMap[new ConfigResource(ConfigResource.Type.TOPIC,topic)].entries()
        // println diffConfigEntryes
        var configResource = new ConfigResource(ConfigResource.Type.TOPIC,topic)
        var diffConfigEntryes = GQ {
            from itemConf in configEntryes
            where !(
                from itemKafka in configEntryMapKafka[configResource].entries()
                where itemKafka.name() == itemConf.name() && itemKafka.value() == itemConf.value()
                select itemKafka
            ).exists()
            select itemConf
        }.toList()
        //println diffConfigEntryes
        if(diffConfigEntryes) {
            alterConfigOpMap[configResource] = diffConfigEntryes.collect { new AlterConfigOp(it, AlterConfigOp.OpType.SET) }
        }


    }
    kafkaAdminClient.incrementalAlterConfigs(alterConfigOpMap).all().get()
    kafkaAdminClient.close()
}

def UpdatePartitionsCount(cluster){
    var kafkaAdminClient = getKafkaAdminClient(cluster)
    var topicDescriptionMap = kafkaAdminClient.describeTopics(getTopicsFromConfig(getConfigFromCsvFile(cluster))).all().get()
    var topicPartitionCountMap = getConfigFromCsvFile(cluster).collectEntries {[it[0],it[4].toInteger()]}
    var partitionsForIncreaseMap = topicPartitionCountMap.collectEntries { key, value ->
        if(value > topicDescriptionMap[key].partitions().size()){
            [key,NewPartitions.increaseTo(value)]
        }
    }
    kafkaAdminClient.createPartitions(partitionsForIncreaseMap,new CreatePartitionsOptions().timeoutMs(300000)).all().get()
    kafkaAdminClient.close()




}

def createAcls(cluster){
    var kafkaAdminClient = getKafkaAdminClient(cluster)
    var aclsConf = getAclsFromConfig(getConfigFromCsvFile(cluster))
    var aclsFromKafka = kafkaAdminClient.describeAcls(AclBindingFilter.ANY).values().get()
    var aclsDiff = aclsConf - aclsFromKafka
    kafkaAdminClient.createAcls(aclsDiff,new CreateAclsOptions().timeoutMs(300000)).all().get()
    kafkaAdminClient.close()
}


//var conf = getConfigFromCsvFile("test")
//println getTopicsFromConfig(conf)
//var acls = getAclsFromConfig(conf)
//var kafkaAdminClient = getKafkaAdminClient("test")
//var aclsFromKafka = kafkaAdminClient.describeAcls(AclBindingFilter.ANY).values().get()

//println acls - aclsFromKafka


//println aclsFromKafka
//createMissingTopics("test")
//println getPartitionsCountFromConf("test","nsi.fct.vlad-data-50.0")
//createMissingTopics("test")
//println getUsersFromKafkaConfig("test")
//println getUsersFromCsvConfig("test")
//addMissingUsers("test")
//updateTopicsConfiguration("test")
//UpdatePartitionsCount("test")


var yamlFile = new File("clusters.yaml")
var yaml = new YamlSlurper().parse(yamlFile)
yaml.keySet().each { cluster ->
    createMissingTopics(cluster)
    createAcls(cluster)
    updateTopicsConfiguration(cluster)
    UpdatePartitionsCount(cluster)
    addMissingUsers(cluster)
}

