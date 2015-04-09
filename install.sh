#!/bin/bash
mvn deploy:deploy-file -DgroupId=com.redis \
  -DartifactId=redis-scala-netty \
  -Dversion=1.2.4 \
  -Dpackaging=jar \
  -Dfile=./bin/redis-scala-netty-1.2.4.jar \
  -Durl=sftp://192.168.2.101:9142/repository \
  -DrepositoryId=ftp-repo-dev \
  -DaltDeploymentRepository=ftp-repo-dev::default::sftp://192.168.2.101:9142/repository/

# mvn deploy -Durl=sftp://192.168.2.101:9142/repository -DrepositoryId=ftp-repo-dev -DaltDeploymentRepository=ftp-repo-dev::default::sftp://192.168.2.101:9142/repository/