Buildr.settings.build['scala.version'] = "2.8.0"

require 'buildr/scala'


VERSION_NUMBER = "1.0.0"
GROUP = "redis-client-scala-netty"
COPYRIGHT = "Fotolog, Inc"

repositories.remote << 'http://scala-tools.org/repo-releases/'
repositories.remote << "http://www.ibiblio.org/maven2/"
repositories.remote << 'http://repository.jboss.org/maven2/'

desc "The Redis-client-scala-netty project"
define "redis-client-scala-netty" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Fotolog"] = COPYRIGHT

  compile.with 'org.jboss.netty:netty:jar:3.1.5.GA'
  compile.with 'apache-log4j:log4j:jar:1.2.15'

  package :jar

end
